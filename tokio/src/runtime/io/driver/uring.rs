use io_uring::{IoUring, squeue::Entry};
use mio::unix::SourceFd;
use slab::Slab;

use crate::io::Interest;
use crate::loom::sync::atomic::Ordering;
use crate::runtime::driver::op::{CancelData, CqeResult};
use crate::sync::oneshot;

use super::{Handle, TOKEN_WAKEUP_ALL};

use std::cell::{OnceCell, RefCell};
use std::io;
use std::os::fd::{AsRawFd, RawFd};

const DEFAULT_RING_SIZE: u32 = 256;

pub(crate) type CqeSender = oneshot::Sender<(CqeResult, CancelData)>;

pub(crate) struct UringContext {
    pub(crate) uring: Option<io_uring::IoUring>,
    pub(crate) ops: slab::Slab<(CqeSender, CancelData)>,
}

pub(crate) enum UringState {
    Initialized(i32),
    Uninitialized,
    Unsupported,
    Disabled,
}

impl UringState {
    pub(crate) fn from_u32(value: u32) -> Self {
        match value {
            i if i == i32::MAX as u32 + 1 => UringState::Uninitialized,
            i if i == i32::MAX as u32 + 2 => UringState::Unsupported,
            i if i == i32::MAX as u32 + 3 => UringState::Disabled,
            fd => UringState::Initialized(fd as i32),
        }
    }

    pub(crate) fn as_u32(&self) -> u32 {
        match self {
            UringState::Initialized(fd) => *fd as u32,
            UringState::Uninitialized => i32::MAX as u32 + 1,
            UringState::Unsupported => i32::MAX as u32 + 2,
            UringState::Disabled => i32::MAX as u32 + 3,
        }
    }
}

impl UringContext {
    pub(crate) fn new() -> Self {
        Self {
            ops: Slab::new(),
            uring: None,
        }
    }

    pub(crate) fn ring(&self) -> &io_uring::IoUring {
        self.uring.as_ref().expect("io_uring not initialized")
    }

    pub(crate) fn ring_mut(&mut self) -> &mut io_uring::IoUring {
        self.uring.as_mut().expect("io_uring not initialized")
    }

    /// Perform `io_uring_setup` system call, and Returns true if this
    /// actually initialized the io_uring.
    ///
    /// If the machine doesn't support io_uring, then this will return an
    /// `ENOSYS` error.
    pub(crate) fn try_init(&mut self, uring_fd: Option<i32>) -> io::Result<bool> {
        let mut uring = IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder();

        if let Some(fd) = uring_fd {
            // Safety: The fd must be a valid io_uring fd.
            uring.setup_attach_wq(fd);
        }

        self.uring.replace(uring.build(DEFAULT_RING_SIZE)?);

        tracing::trace!("io_uring initialized with fd {}", self.ring().as_raw_fd());
        Ok(true)
    }

    pub(crate) fn dispatch_completions(&mut self) -> usize {
        let ops = &mut self.ops;
        let Some(mut uring) = self.uring.take() else {
            // Uring is not initialized yet.
            return 0;
        };

        let mut cq = uring.completion();
        cq.sync();

        let mut dispatched = 0;

        for cqe in cq {
            dispatched += 1;
            let idx = cqe.user_data() as usize;

            match ops.try_remove(idx) {
                Some((sender, data)) => {
                    // It's possible that the receiver has been dropped, so we ignore the error.
                    let _ = sender.send((CqeResult::from(cqe), data));
                }
                None => {
                    panic!("no op at index {idx}");
                }
            }
        }

        if dispatched > 0 {
            tracing::trace!("dispatched {} completions", dispatched);
        }
        
        self.uring.replace(uring);

        dispatched

        // `cq`'s drop gets called here, updating the latest head pointer
    }

    pub(crate) fn submit(&mut self) -> io::Result<()> {
        loop {
            // Errors from io_uring_enter: https://man7.org/linux/man-pages/man2/io_uring_enter.2.html#ERRORS
            match self.ring().submit() {
                Ok(_) => {
                    return Ok(());
                }

                // If the submission queue is full, we dispatch completions and try again.
                Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {
                    self.dispatch_completions();
                }
                // For other errors, we currently return the error as is.
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    pub(crate) fn remove_op(&mut self, index: usize) -> (CqeSender, CancelData) {
        self.ops.remove(index)
    }
}

/// Drop the driver, cancelling any in-progress ops and waiting for them to terminate.
impl Drop for UringContext {
    fn drop(&mut self) {
        if self.uring.is_none() {
            // Uring is not initialized or not supported.
            return;
        }

        // Make sure we flush the submission queue before dropping the driver.
        while !self.ring_mut().submission().is_empty() {
            self.submit().expect("Internal error when dropping driver");
        }

        let mut ops = std::mem::take(&mut self.ops);

        while !ops.is_empty() {
            // Wait until at least one completion is available.
            self.ring_mut()
                .submit_and_wait(1)
                .expect("Internal error when dropping driver");

            for cqe in self.ring_mut().completion() {
                let idx = cqe.user_data() as usize;
                ops.remove(idx);
            }
        }
    }
}

tokio_thread_local!(static URING_CTX: OnceCell<RefCell<UringContext>> = OnceCell::new());

impl Handle {
    fn add_uring_source(&self, uringfd: RawFd) -> io::Result<()> {
        let mut source = SourceFd(&uringfd);
        tracing::trace!("registering uring fd {uringfd} with mio");
        self.registry
            .register(&mut source, TOKEN_WAKEUP_ALL, Interest::READABLE.to_mio())
    }

    pub(crate) fn with_uring<F, R>(&self, f: F) -> Result<R, io::Error>
    where
        F: FnOnce(&mut UringContext) -> R,
    {
        URING_CTX.with(|cell| {
            let mut err = None;

            let ctx = cell.get_or_init(|| {
                let mut ctx = UringContext::new();
                let uring_state = UringState::from_u32(self.uring_fd.load(Ordering::Acquire));

                if matches!(
                    uring_state,
                    UringState::Uninitialized | UringState::Initialized(_)
                ) {
                    let uring_fd = match uring_state {
                        UringState::Initialized(fd) => Some(fd),
                        _ => None,
                    };

                    if let Err(e) = ctx.try_init(uring_fd) {
                        if matches!(uring_state, UringState::Initialized(_)) {
                            // failed to initialize uring on this worker thread, after it was successful on another
                            panic!(
                                "io_uring was initialized on another thread, but failed to initialize on this thread: {err:?}"
                            );
                        }

                        // If the error is ENOSYS, then the kernel doesn't support io_uring.
                        if e.raw_os_error() == Some(libc::ENOSYS) {
                            self.uring_fd
                                .store(UringState::Unsupported.as_u32(), Ordering::Relaxed);
                            // we dont propagate this error
                        } else {
                            err = Some(e);
                        }
                    } else {
                        let fd = ctx.ring().as_raw_fd();
                        if let Err(e) = self.uring_fd.compare_exchange(
                            uring_state.as_u32(),
                            UringState::Initialized(fd).as_u32(),
                            Ordering::Acquire,
                            Ordering::Acquire,
                        ) {
                            
                            let new_state = UringState::from_u32(e);
                            if let UringState::Initialized(fd) = new_state {
                                if let Err(e) = ctx.try_init(Some(fd)) {
                                    err = Some(e);
                                }
                            } else {
                                // this means that this thread was successful in initializing the uring,
                                // but a different thread received ENOSYS in the meantime.
                                unreachable!(
                                    "first uring initialize succeeded, but a concurrent one was unsupported"
                                )
                            }
                        }
                    }

                    if err.is_none() {
                        if let Err(e) = self.add_uring_source(ctx.ring().as_raw_fd()) {
                            err = Some(e);
                        }
                    }
                }

                RefCell::new(ctx)
            });

            if let Some(e) = err {
                tracing::trace!("uring initialization failed: {e:?}");
                return Err(e);
            }

            Ok(f(&mut ctx.borrow_mut()))
        })
    }

    /// Check if the io_uring context is available. If uninitialized, it will try to initialize it.
    pub(crate) fn check_and_init(&self) -> io::Result<bool> {
        // self.with_uring(|ctx| ctx.uring.is_some())?;

        let uring_state = UringState::from_u32(self.uring_fd.load(Ordering::Acquire));

        match uring_state {
            UringState::Initialized(_) => {
                // Already initialized.
                Ok(true)
            }
            UringState::Unsupported => {
                // Not supported on this machine.
                Ok(false)
            }
            UringState::Disabled => {
                // Disabled manually.
                Ok(false)
            }
            UringState::Uninitialized => {
                // Try to initialize, then check again.
                self.with_uring(|_| {})?;
                self.check_and_init()
            }
        }
    }

    /// Register an operation with the io_uring.
    ///
    /// If this is the first io_uring operation, it will also initialize the io_uring context.
    /// If io_uring isn't supported, this function returns an `ENOSYS` error, so the caller can
    /// perform custom handling, such as falling back to an alternative mechanism.
    ///
    /// # Safety
    ///
    /// Callers must ensure that parameters of the entry (such as buffer) are valid and will
    /// be valid for the entire duration of the operation, otherwise it may cause memory problems.
    pub(crate) unsafe fn register_op(
        &self,
        entry: Entry,
        sender: CqeSender,
        cancel_data: CancelData,
    ) -> Result<usize, (io::Error, CancelData)> {
        // Note: Maybe this check can be removed if upstream callers consistently use `check_and_init`.
        let check = self.check_and_init();
        if let Err(e) = check {
            return Err((e, cancel_data));
        }
        if !check.unwrap() {
            return Err((io::Error::from_raw_os_error(libc::ENOSYS), cancel_data));
        }

        // Uring is initialized.

        tracing::trace!("registering uring op {:?}", &entry);

        self.with_uring(|ctx| {
            let index = ctx.ops.insert((sender, cancel_data));
            let entry = entry.user_data(index as u64);

            let submit_or_remove = |ctx: &mut UringContext| -> Result<(), (io::Error, CancelData)> {
                if let Err(e) = ctx.submit() {
                    // Submission failed, remove the entry from the slab and return the error
                    let (_, data) = ctx.remove_op(index);
                    return Err((e, data));
                }
                Ok(())
            };

            // SAFETY: entry is valid for the entire duration of the operation
            while unsafe { ctx.ring_mut().submission().push(&entry).is_err() } {
                // If the submission queue is full, flush it to the kernel
                submit_or_remove(ctx)?;
            }

            // Ensure that the completion queue is not full before submitting the entry.
            while ctx.ring_mut().completion().is_full() {
                ctx.dispatch_completions();
            }

            // Note: For now, we submit the entry immediately without utilizing batching.
            submit_or_remove(ctx)?;

            Ok(index)
        })
        .expect("uring was checked as initialized")
    }
}

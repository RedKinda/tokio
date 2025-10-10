use io_uring::{IoUring, squeue::Entry};
use mio::unix::SourceFd;
use slab::Slab;

use crate::loom::sync::atomic::Ordering;
use crate::runtime::driver::op::{CancelData, CqeResult};
use crate::sync::oneshot;
use crate::{io::Interest, loom::sync::Mutex};

use super::{Handle, TOKEN_WAKEUP};

use std::cell::{OnceCell, RefCell};
use std::io;
use std::os::fd::{AsRawFd, RawFd};

const DEFAULT_RING_SIZE: u32 = 256;

#[repr(usize)]
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum State {
    Uninitialized = 0,
    Initialized = 1,
    Unsupported = 2,
}

impl State {
    fn as_usize(&self) -> usize {
        *self as usize
    }

    fn from_usize(value: usize) -> Self {
        match value {
            0 => State::Uninitialized,
            1 => State::Initialized,
            2 => State::Unsupported,
            _ => unreachable!("invalid Uring state: {}", value),
        }
    }
}

pub(crate) type CqeSender = oneshot::Sender<(CqeResult, CancelData)>;

pub(crate) struct UringContext {
    pub(crate) uring: Option<io_uring::IoUring>,
    pub(crate) ops: slab::Slab<(CqeSender, CancelData)>,
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

        Ok(true)
    }

    pub(crate) fn dispatch_completions(&mut self) {
        let ops = &mut self.ops;
        let Some(mut uring) = self.uring.take() else {
            // Uring is not initialized yet.
            return;
        };

        let mut cq = uring.completion();
        cq.sync();

        for cqe in cq {
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

        self.uring.replace(uring);

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

    pub(crate) fn remove_op(&mut self, index: usize) {
        self.ops.remove(index);
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
        self.registry
            .register(&mut source, TOKEN_WAKEUP, Interest::READABLE.to_mio())
    }

    pub(crate) fn with_uring<F, R>(&self, f: F) -> Result<R, io::Error>
    where
        F: FnOnce(&mut UringContext) -> R,
    {
        URING_CTX.with(|cell| {
            let mut err = None;
            let ctx = cell.get_or_init(|| {
                let mut ctx = UringContext::new();
                let uring_fd = self.uring_fd.load(Ordering::Acquire);
                let uring_fd = if uring_fd == 0 {
                    None
                } else {
                    Some(uring_fd as i32)
                };

                if let Err(e) = ctx.try_init(uring_fd) {
                    err = Some(e);
                } else {
                    let fd = ctx.ring().as_raw_fd();
                    if let Err(e) =
                        self.uring_fd
                            .compare_exchange(0, fd, Ordering::Acquire, Ordering::Acquire)
                    {
                        // Another thread initialized the uring_fd concurrently.
                        // We re-initialize the context with the existing fd.
                        if let Err(e) = ctx.try_init(Some(e)) {
                            err = Some(e);
                        }
                    }
                }

                if err.is_none() {
                    if let Err(e) = self.add_uring_source(ctx.ring().as_raw_fd()) {
                        err = Some(e);
                    }
                }

                RefCell::new(ctx)
            });

            // TODO make this not panic
            if let Some(e) = err {
                return Err(e);
            }

            Ok(f(&mut ctx.borrow_mut()))
        })
    }

    /// Check if the io_uring context is initialized. If not, it will try to initialize it.
    pub(crate) fn check_and_init(&self) -> io::Result<bool> {
        self.with_uring(|ctx| ctx.uring.is_some())?;

        Ok(true)
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
    ) -> io::Result<usize> {
        // Note: Maybe this check can be removed if upstream callers consistently use `check_and_init`.
        if !self.check_and_init()? {
            return Err(io::Error::from_raw_os_error(libc::ENOSYS));
        }

        // Uring is initialized.

        self.with_uring(|ctx| {
            let index = ctx.ops.insert((sender, cancel_data));
            let entry = entry.user_data(index as u64);

            let submit_or_remove = |ctx: &mut UringContext| -> io::Result<()> {
                if let Err(e) = ctx.submit() {
                    // Submission failed, remove the entry from the slab and return the error
                    ctx.remove_op(index);
                    return Err(e);
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

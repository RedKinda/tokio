use io_uring::{IoUring, squeue::Entry};
use mio::unix::SourceFd;
use slab::Slab;

use crate::io::Interest;
use crate::runtime::driver::op::{CancelData, CqeResult};
use crate::runtime::io::scheduled_io::ScheduledIo;
use crate::sync::oneshot;

use super::Handle;

use std::cell::{OnceCell, RefCell};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

const DEFAULT_RING_SIZE: u32 = 256;

pub(crate) type CqeSender = oneshot::Sender<(CqeResult, CancelData)>;

pub(crate) struct UringContext {
    inner: Result<UringContextInner, io::Error>,
}

pub(crate) struct UringContextInner {
    pub(crate) uring: io_uring::IoUring,
    pub(crate) ops: slab::Slab<(CqeSender, CancelData)>,
    io_waking: (Waker, Arc<ScheduledIo>),
}

#[derive(Clone, Debug)]
pub(crate) enum UringState {
    Initialized(i32),
    Uninitialized,
    Unsupported,
    Disabled,
}
impl UringState {
    pub(crate) fn is_disabled(&self) -> bool {
        matches!(self, UringState::Disabled)
    }

    pub(crate) fn is_unsupported(&self) -> bool {
        matches!(self, UringState::Unsupported)
    }

    pub(crate) fn is_initialized(&self) -> bool {
        matches!(self, UringState::Initialized(_))
    }
}

impl UringContext {
    pub(crate) fn new(uring_wq_fd: Option<i32>, waker: Waker, handle: &Handle) -> Self {
        Self {
            inner: UringContextInner::try_init(uring_wq_fd, waker, handle),
        }
    }
}

impl UringContextInner {
    pub(crate) fn try_init(
        uring_wq_fd: Option<i32>,
        waker: Waker,
        handle: &Handle,
    ) -> io::Result<Self> {
        let builder = |uring_wq_fd: Option<i32>| {
            let mut uring = IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder();

            if let Some(fd) = uring_wq_fd {
                // Safety: The fd must be a valid io_uring fd.
                uring.setup_attach_wq(fd);
            }

            uring.setup_single_issuer();
            uring.setup_coop_taskrun();

            uring.build(DEFAULT_RING_SIZE)
        };

        let uring = match builder(uring_wq_fd) {
            Ok(uring) => uring,
            Err(_) => {
                // code 6
                builder(None)?
            }
        };

        let scheduled_io = handle.add_uring_source(uring.as_raw_fd())?;

        let s = Self {
            uring,
            ops: Slab::with_capacity(DEFAULT_RING_SIZE as usize),
            io_waking: (waker, scheduled_io),
        };

        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!(
            "io_uring initialized with fd {} - WQ {:?}",
            s.uring.as_raw_fd(),
            uring_wq_fd
        );

        Ok(s)
    }

    pub(crate) fn dispatch_completions(&mut self) -> usize {
        let ops = &mut self.ops;

        let mut dispatched = 0;

        // if uring is initialized, io_waking must be initialized too
        let (waker, scheduled_io) = &self.io_waking;
        let mut cx = Context::from_waker(waker);
        // if uring receives new completions while we are dispatching them we loop
        // once its Poll::Pending, ScheduledIo has our thread waker so we will be notified on new completions

        let mut do_dispatch = || {
            let mut cq = self.uring.completion();
            cq.sync();

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
        };

        let mut ran_once = false;
        while let Poll::Ready(ready) = scheduled_io.poll_readiness(&mut cx, super::Direction::Read)
        {
            scheduled_io.clear_readiness(ready);
            do_dispatch();
            ran_once = true;
        }

        // ensure we always dispatch at least once
        if !ran_once {
            // do_dispatch();
        }

        #[cfg(all(tokio_unstable, feature = "tracing"))]
        if dispatched > 0 {
            tracing::trace!("dispatched {} completions", dispatched);
        } else {
            tracing::trace!("no completions to dispatch");
        }

        dispatched

        // `cq`'s drop gets called here, updating the latest head pointer
    }

    pub(crate) fn submit(&mut self) -> io::Result<()> {
        loop {
            // Errors from io_uring_enter: https://man7.org/linux/man-pages/man2/io_uring_enter.2.html#ERRORS
            match self.uring.submit() {
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
impl Drop for UringContextInner {
    fn drop(&mut self) {
        // Make sure we flush the submission queue before dropping the driver.
        while !self.uring.submission().is_empty() {
            self.submit().expect("Internal error when dropping driver");
        }

        let mut ops = std::mem::take(&mut self.ops);

        while !ops.is_empty() {
            // Wait until at least one completion is available.
            self.uring
                .submit_and_wait(1)
                .expect("Internal error when dropping driver");

            for cqe in self.uring.completion() {
                let idx = cqe.user_data() as usize;
                ops.remove(idx);
            }
        }
    }
}

tokio_thread_local!(static URING_CTX: OnceCell<RefCell<UringContext>> = OnceCell::new());

impl Handle {
    fn add_uring_source(&self, uringfd: RawFd) -> io::Result<Arc<ScheduledIo>> {
        let mut source = SourceFd(&uringfd);
        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!("registering uring fd {uringfd} with mio");

        self.add_source(&mut source, Interest::READABLE)

        // self.registry
        //     .register(&mut source, TOKEN_WAKEUP_ALL, Interest::READABLE.to_mio())
    }

    pub(crate) fn with_uring<F, R>(&self, f: F) -> Result<R, io::Error>
    where
        F: FnOnce(&mut UringContextInner) -> R,
    {
        URING_CTX.with(|cell| {
            let cell = cell.get();

            if cell.is_none() {
                // this should be unreachable
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "io_uring context is not yet initialized",
                ));
            }

            let mut ctx = cell.unwrap().borrow_mut();
            match &mut ctx.inner {
                Ok(uring_inner) => Ok(f(uring_inner)),
                Err(e) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("io_uring context previously failed to initialize: {e:?}"),
                )),
            }
        })
    }

    pub(crate) fn init_uring(&self, waker: Waker) -> io::Result<bool> {
        let mut uring_state = self.uring_state.lock();

        // user indicated they don't want to use uring, so we won't even try
        if uring_state.is_disabled() || uring_state.is_unsupported() {
            return Ok(false);
        }

        URING_CTX.with(|cell| {
            let initter = || {
                let uring_wq_fd = match *uring_state {
                    UringState::Initialized(fd) => Some(fd),
                    _ => None,
                };

                let ctx = UringContext::new(uring_wq_fd, waker.clone(), self);

                if let Err(e) = &ctx.inner {
                    if let UringState::Initialized(other_fd) = *uring_state {
                        // failed to initialize uring on this worker thread, after it was successful on another
                        panic!(
                            "io_uring was initialized on another thread, but failed to initialize on this thread: {e:?} - other thread initialized on fd {other_fd:?}"
                        );
                    }

                    let pass_error = io::Error::new(
                        io::ErrorKind::Other,
                        format!("io_uring context failed to initialize: {e:?}"),
                    );

                    return Err((pass_error, ctx));
                }

                Ok(RefCell::new(ctx))
            };

            // Some(Some(e)) if initialization failed with error e
            // Some(None) if initialization succeeded
            // None if cell was already initialized
            let mut err: Option<Option<io::Error>> = None;

            let cell_init = || match initter() {
                Ok(c) => {
                    err = Some(None);
                    c
                }
                Err((e, ctx)) => {
                    err = Some(Some(e));
                    RefCell::new(ctx)
                }
            };

            let ctx = cell.get_or_init(cell_init);

            match err {
                // error
                Some(Some(e)) => {
                    // if error is ENOSYS, we mark uring as unsupported
                    if e.raw_os_error() == Some(libc::ENOSYS) {
                        *uring_state = UringState::Unsupported;
                    }

                    #[cfg(all(tokio_unstable, feature = "tracing"))]
                    tracing::trace!("uring initialization failed: {e:?}");
                    return Err(e);
                }
                // cell was just now inited, and succeeded
                Some(None) => {
                    *uring_state = UringState::Initialized(
                        ctx.borrow().inner.as_ref().unwrap().uring.as_raw_fd(),
                    );
                }
                // cell was inited previously
                None => {
                    // if it previously failed to init, we error out
                    if let Err(e) = &ctx.borrow().inner {
                        if e.raw_os_error() == Some(libc::ENOSYS) {
                            *uring_state = UringState::Unsupported;
                        }

                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("io_uring context previously failed to initialize: {e:?}"),
                        ));
                    }

                    // otherwise we need to update the uring waker and register a new scheduledio
                    let mut c = ctx.borrow_mut();
                    let uring_inner = c.inner.as_mut().unwrap();
                    // we dont need to update the ScheduledIo because we only care about the waker
                    // which will be updated on the next poll_readiness call
                    let scheduled_io = self.add_uring_source(uring_inner.uring.as_raw_fd());
                    uring_inner.io_waking.0 = waker;
                    if let Ok(scheduled_io) = scheduled_io {
                        uring_inner.io_waking.1 = scheduled_io
                    }

                    *uring_state = UringState::Initialized(uring_inner.uring.as_raw_fd());
                }
            }

            Ok(uring_state.is_initialized())
        })
    }

    /// Check if the io_uring context is available
    pub(crate) fn check_uring(&self) -> io::Result<bool> {
        let uring_state = self.uring_state.lock().clone();

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
            UringState::Uninitialized => Err(io::Error::new(
                io::ErrorKind::Other,
                "io_uring context is not initialized",
            )),
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
        let check = self.check_uring();
        if let Err(e) = check {
            return Err((e, cancel_data));
        }
        if !check.unwrap() {
            return Err((io::Error::from_raw_os_error(libc::ENOSYS), cancel_data));
        }

        // Uring is initialized.

        self.with_uring(|ctx| {
            let index = ctx.ops.insert((sender, cancel_data));
            let entry = entry.user_data(index as u64);

            let submit_or_remove =
                |ctx: &mut UringContextInner| -> Result<(), (io::Error, CancelData)> {
                    if let Err(e) = ctx.submit() {
                        // Submission failed, remove the entry from the slab and return the error
                        let (_, data) = ctx.remove_op(index);
                        return Err((e, data));
                    }
                    Ok(())
                };

            // SAFETY: entry is valid for the entire duration of the operation
            while unsafe { ctx.uring.submission().push(&entry).is_err() } {
                // If the submission queue is full, flush it to the kernel
                submit_or_remove(ctx)?;
            }

            // Ensure that the completion queue is not full before submitting the entry.
            while ctx.uring.completion().is_full() {
                ctx.dispatch_completions();
            }

            // Note: For now, we submit the entry immediately without utilizing batching.
            submit_or_remove(ctx)?;

            Ok(index)
        })
        .expect("uring was checked as initialized")
    }
}

use crate::io::uring::open::Open;
use crate::io::uring::write::Write;
use crate::runtime::Handle;
use crate::sync::oneshot;
use io_uring::cqueue;
use io_uring::squeue::Entry;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

// This field isn't accessed directly, but it holds cancellation data,
// so `#[allow(dead_code)]` is needed.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CancelData {
    Open(Open),
    Write(Write),
}

pub(crate) enum State {
    Initialize(Option<Entry>),
    Polled(oneshot::Receiver<(CqeResult, CancelData)>),
    Complete,
}

pub(crate) struct Op<T: Cancellable> {
    // Handle to the runtime
    handle: Handle,
    // State of this Op
    state: State,
    // Per operation data.
    data: Option<T>,
}

impl<T: Cancellable> Op<T> {
    /// # Safety
    ///
    /// Callers must ensure that parameters of the entry (such as buffer) are valid and will
    /// be valid for the entire duration of the operation, otherwise it may cause memory problems.
    pub(crate) unsafe fn new(entry: Entry, data: T) -> Self {
        let handle = Handle::current();
        Self {
            handle,
            data: Some(data),
            state: State::Initialize(Some(entry)),
        }
    }
    pub(crate) fn take_data(&mut self) -> Option<T> {
        self.data.take()
    }
}

/// A single CQE result
pub(crate) struct CqeResult {
    pub(crate) result: io::Result<u32>,
}

impl From<cqueue::Entry> for CqeResult {
    fn from(cqe: cqueue::Entry) -> Self {
        let res = cqe.result();
        let result = if res >= 0 {
            Ok(res as u32)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        };
        CqeResult { result }
    }
}

/// A trait that converts a CQE result into a usable value for each operation.
pub(crate) trait Completable {
    type Output;
    type Error;
    fn complete(self, cqe: CqeResult) -> Result<Self::Output, (io::Error, Self::Error)>;
    fn error(self) -> Self::Error;
}

/// Extracts the `CancelData` needed to safely cancel an in-flight io_uring operation.
pub(crate) trait Cancellable {
    fn cancel_data(self) -> CancelData;
    fn from_data(data: CancelData) -> Self
    where
        Self: Sized;
}

impl<T: Cancellable> Unpin for Op<T> {}

impl<T: Cancellable + Completable + Send> Future for Op<T> {
    type Output = Result<T::Output, (io::Error, T::Error)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match &mut this.state {
            State::Initialize(entry_opt) => {
                let entry = entry_opt.take().expect("Entry must be present");
                let (tx, rx) = oneshot::channel();
                let data = this
                    .take_data()
                    .expect("Data must be some when initializing")
                    .cancel_data();

                let handle = &mut this.handle;
                let driver = handle.inner.driver().io();

                // SAFETY: entry is valid for the entire duration of the operation
                unsafe {
                    driver.register_op(entry, tx, data).map_err(|e| {
                        // If registration fails, we need to return the data back to the caller
                        let data = T::from_data(e.1).error();
                        (e.0, data)
                    })?
                };

                this.state = State::Polled(rx);

                // op has been submitted, we drive completions in case it completed immediately
                let _ = driver.with_uring(|uring| uring.dispatch_completions());

                // immediately poll self again so that rx is polled and a waker is registered
                pin!(this);
                this.poll(cx)
            }

            State::Polled(rx) => {
                // poll the receiver
                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok((cqe, data))) => {
                        this.state = State::Complete;
                        let d = T::from_data(data).complete(cqe);
                        Poll::Ready(d)
                    }
                    Poll::Ready(Err(_)) => {
                        // The sender was dropped, which means the operation was cancelled.
                        // This shouldnt happen, maybe panic here instead?
                        this.state = State::Complete;
                        panic!("oneshot sender dropped, operation was cancelled");
                        // Poll::Ready(Err(io::Error::new(
                        //     io::ErrorKind::Other,
                        //     "operation cancelled",
                        // )))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }

            State::Complete => {
                panic!("Future polled after completion");
            }
        }
    }
}

use crate::io::uring::open::Open;
use crate::io::uring::read::Read;
use crate::io::uring::write::Write;
use crate::runtime::Handle;
use crate::sync::oneshot;
use io_uring::cqueue;
use io_uring::squeue::Entry;
use std::any::type_name;
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
    Read(Read),
}

pub(crate) enum State {
    Initialize(Option<Entry>),
    Polled(oneshot::Receiver<(CqeResult, CancelData)>),
    Complete,
}

pub(crate) struct Op<T: Cancellable> {
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
        Self {
            data: Some(data),
            state: State::Initialize(Some(entry)),
        }
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

impl<T: Cancellable + Completable + Send + std::fmt::Debug> Future for Op<T> {
    type Output = Result<T::Output, (io::Error, T::Error)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match &mut this.state {
            State::Initialize(entry_opt) => {
                let (entry, data) = match (entry_opt.take(), this.data.take()) {
                    (Some(e), Some(d)) => (e, d),
                    _ => panic!("Entry and Data must be present when initializing"),
                };

                let (tx, rx) = oneshot::channel();

                crate::runtime::io::uring::with_current_uring(|uring| {
                    #[cfg(all(tokio_unstable, feature = "tracing"))]
                    tracing::trace!("registering uring op {} - {:?}", type_name::<T>(), &entry);

                    // SAFETY: entry is valid for the entire duration of the operation
                    unsafe {
                        if let Err(e) = uring.register_op(entry, tx, data.cancel_data()) {
                            // If registration fails, we need to return the data back to the caller
                            return Poll::Ready(Err((e.0, T::from_data(e.1).error())));
                        }
                    };

                    this.state = State::Polled(rx);

                    // op has been submitted, we drive completions in case it completed immediately, as this happens quite often
                    let _ = uring.dispatch_completions(false);

                    // immediately poll self again so that rx is polled and a waker is registered
                    pin!(this);
                    this.poll(cx)
                })
                .expect("uring is always available here")
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

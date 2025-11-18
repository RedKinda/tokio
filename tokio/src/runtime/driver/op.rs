use crate::io::uring::open::Open;
use crate::io::uring::read::Read;
use crate::io::uring::write::Write;
use crate::io::uring::write::WriteVectored;
use io_uring::cqueue;
use io_uring::squeue::Entry;
use std::future::Future;
use std::io;
use std::mem;
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
    WriteVectored(WriteVectored),
    Read(Read),
}

pub(crate) enum State<T: Cancellable> {
    Initialize(Entry, T),
    Polled(oneshot::Receiver<(CqeResult, CancelData)>),
    CompleteOrInvalid,
}

pub(crate) struct Op<T: Cancellable> {
    // State of this Op
    state: State<T>,
}

impl<T: Cancellable> Op<T> {
    /// # Safety
    ///
    /// Callers must ensure that parameters of the entry (such as buffer) are valid and will
    /// be valid for the entire duration of the operation, otherwise it may cause memory problems.
    pub(crate) unsafe fn new(entry: Entry, data: T) -> Self {
        Self {
            state: State::Initialize(entry, data),
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

        match mem::replace(&mut this.state, State::CompleteOrInvalid) {
            State::Initialize(entry, data) => {
                let (tx, rx) = oneshot::channel();

                crate::runtime::io::uring::with_current_uring(|uring| {
                    #[cfg(all(tokio_unstable, feature = "tracing"))]
                    tracing::trace!(
                        "registering uring op {} - {:?}",
                        std::any::type_name::<T>(),
                        &entry
                    );

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

            State::Polled(mut rx) => {
                // poll the receiver
                match Pin::new(&mut rx).poll(cx) {
                    Poll::Ready(Ok((cqe, data))) => {
                        this.state = State::CompleteOrInvalid;
                        let d = T::from_data(data).complete(cqe);
                        Poll::Ready(d)
                    }
                    Poll::Ready(Err(_)) => {
                        // The sender was dropped, which means the operation was cancelled.
                        // This shouldnt happen, maybe panic here instead?
                        this.state = State::CompleteOrInvalid;
                        panic!("oneshot sender dropped, operation was cancelled");
                        // Poll::Ready(Err(io::Error::new(
                        //     io::ErrorKind::Other,
                        //     "operation cancelled",
                        // )))
                    }
                    Poll::Pending => {
                        this.state = State::Polled(rx);
                        Poll::Pending
                    }
                }
            }

            State::CompleteOrInvalid => {
                panic!("Future polled after completion");
            }
        }
    }
}

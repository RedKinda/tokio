use crate::{
    io::blocking::Buf,
    runtime::driver::op::{CancelData, Cancellable, Completable, CqeResult, Op},
};
use io_uring::{opcode, types};
use std::{
    any::Any,
    io::{self},
    os::fd::AsRawFd,
    sync::Arc,
};

#[cfg(test)]
use crate::fs::mocks::MockFile as StdFile;
#[cfg(not(test))]
use std::fs::File as StdFile;

pub(crate) struct Write {
    buf: Buf,
    file: Arc<dyn AsRawFd + Sync + Send>,
}

impl std::fmt::Debug for Write {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("buf", &self.buf)
            .field("fd", &self.file.as_raw_fd())
            .finish()
    }
}

impl Completable for Write {
    type Output = (u32, Buf, Arc<dyn AsRawFd + Sync + Send>);
    type Error = (Buf, Arc<dyn AsRawFd + Sync + Send>);
    fn complete(mut self, cqe: CqeResult) -> Result<Self::Output, (io::Error, Self::Error)> {
        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!("write complete: {:?}", cqe.result);

        match cqe.result {
            Ok(n) => {
                self.buf.advance(n as usize);
                Ok((n, self.buf, self.file))
            }
            Err(e) => Err((e, self.error())),
        }
    }
    fn error(self) -> Self::Error {
        (self.buf, self.file)
    }
}

impl Cancellable for Write {
    fn cancel_data(self) -> CancelData {
        CancelData::Write(self)
    }

    fn from_data(data: CancelData) -> Self
    where
        Self: Sized,
    {
        match data {
            CancelData::Write(write) => write,
            _ => panic!("unexpected CancelData variant"),
        }
    }
}

impl Op<Write> {
    /// Issue a write that starts at `buf_offset` within `buf` and writes some bytes
    /// into `file` at `file_offset`.
    pub(crate) fn write_at(
        file: Arc<dyn AsRawFd + Sync + Send>,
        buf: Buf,
        file_offset: u64,
    ) -> Self {
        // There is a cap on how many bytes we can write in a single uring write operation.
        // ref: https://github.com/axboe/liburing/discussions/497
        let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);

        let ptr = buf.bytes().as_ptr();

        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!("registering write op len: {len}, file offset: {file_offset}");

        let sqe = opcode::Write::new(types::Fd(file.as_raw_fd()), ptr, len)
            .offset(file_offset)
            .build();

        // SAFETY: `buf`` is owned by this operation, and is therefore valid for the entirety of it
        // file is Arc<StdFile> so it is also valid for the entirety of the operation
        unsafe { Op::new(sqe, Write { buf, file }) }
    }
}

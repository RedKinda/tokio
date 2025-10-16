use crate::{
    io::blocking::Buf,
    runtime::driver::op::{CancelData, Cancellable, Completable, CqeResult, Op},
};
use io_uring::{opcode, types};
use std::{
    io::{self},
    os::fd::AsRawFd,
    sync::Arc,
};

#[cfg(test)]
use crate::fs::mocks::MockFile as StdFile;
#[cfg(not(test))]
use std::fs::File as StdFile;

pub(crate) struct Read {
    buf: Vec<u8>,
    file: Arc<dyn AsRawFd + Sync + Send>,
}

impl std::fmt::Debug for Read {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Read")
            .field("buf_len", &self.buf.len())
            .field("buf_capacity", &self.buf.capacity())
            .field("fd", &self.file.as_raw_fd())
            .finish()
    }
}

impl Completable for Read {
    type Output = (u32, Vec<u8>, Arc<dyn AsRawFd + Sync + Send>);
    type Error = (Vec<u8>, Arc<dyn AsRawFd + Sync + Send>);
    fn complete(mut self, cqe: CqeResult) -> Result<Self::Output, (io::Error, Self::Error)> {
        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!("read complete: {:?}", cqe.result);
        match cqe.result {
            Ok(n) => {
                let len = self.buf.len();
                let new_len = len + n as usize;
                // SAFETY: we trust the kernel to have written `n` bytes to the buffer
                unsafe {
                    self.buf.set_len(new_len);
                }
                Ok((n, self.buf, self.file))
            }
            Err(e) => Err((e, self.error())),
        }
    }
    fn error(self) -> Self::Error {
        (self.buf, self.file)
    }
}

impl Cancellable for Read {
    fn cancel_data(self) -> CancelData {
        CancelData::Read(self)
    }

    fn from_data(data: CancelData) -> Self
    where
        Self: Sized,
    {
        match data {
            CancelData::Read(read) => read,
            _ => panic!("unexpected CancelData variant"),
        }
    }
}

impl Op<Read> {
    /// Reads some bytes from a file at a given offset into a Vec<u8>.
    /// It reads at most Vec::capacity() bytes, and appends data to the Vec.
    /// There is probably a better type for this than Vec, but for now this
    /// works.
    pub(crate) fn read_at(
        file: Arc<dyn AsRawFd + Sync + Send>,
        mut buf: Vec<u8>,
        file_offset: u64,
    ) -> Self {
        let len: usize = buf.len();
        let capacity = buf.capacity();

        // ptr should point at first uninitialized byte
        let ptr = unsafe { buf.as_mut_ptr().add(len) };

        let read_len = capacity - len;

        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!("registering read op len: {read_len}, file offset: {file_offset}");

        let sqe = opcode::Read::new(types::Fd(file.as_raw_fd()), ptr, read_len as u32)
            .offset(file_offset)
            .build();

        // SAFETY: `buf`` is owned by this operation, and is therefore valid for the entirety of it
        // file is Arc<dyn AsRawFd + Sync + Send> so it is also valid for the entirety of the operation
        unsafe { Op::new(sqe, Read { buf, file }) }
    }
}

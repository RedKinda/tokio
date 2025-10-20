use crate::{
    io::blocking::Buf,
    runtime::driver::op::{CancelData, Cancellable, Completable, CqeResult, Op},
};
use io_uring::{opcode, types};
use std::{
    any::Any,
    io::{self, IoSlice},
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

pub(crate) struct WriteVectored {
    buf: Box<[Buf]>,
    // this is actually iovecs, but we don't need to access it after submission
    // we just need to keep it alive while kernel is using it
    iovecs: Box<[u8]>,
    file: Arc<dyn AsRawFd + Sync + Send>,
}

impl std::fmt::Debug for WriteVectored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteVectored")
            .field("buf", &self.buf)
            .field("fd", &self.file.as_raw_fd())
            .finish()
    }
}

impl Completable for WriteVectored {
    type Output = (u32, Box<[Buf]>, Arc<dyn AsRawFd + Sync + Send>);
    type Error = (Box<[Buf]>, Arc<dyn AsRawFd + Sync + Send>);
    fn complete(mut self, cqe: CqeResult) -> Result<Self::Output, (io::Error, Self::Error)> {
        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!("writev complete: {:?}", cqe.result);

        match cqe.result {
            Ok(n) => {
                let mut remaining = n as usize;
                for buf in self.buf.iter_mut() {
                    if remaining == 0 {
                        break;
                    }
                    let to_advance = std::cmp::min(remaining, buf.len());
                    buf.advance(to_advance);
                    remaining -= to_advance;
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
impl Cancellable for WriteVectored {
    fn cancel_data(self) -> CancelData {
        CancelData::WriteVectored(self)
    }

    fn from_data(data: CancelData) -> Self
    where
        Self: Sized,
    {
        match data {
            CancelData::WriteVectored(write) => write,
            _ => panic!("unexpected CancelData variant"),
        }
    }
}

impl Op<WriteVectored> {
    pub(crate) fn write_at_vectored(
        file: Arc<dyn AsRawFd + Sync + Send>,
        bufs: Box<[Buf]>,
        file_offset: u64,
    ) -> Self {
        let iobufs = bufs
            .iter()
            .map(|b| libc::iovec {
                iov_base: b.bytes().as_ptr() as *mut _,
                iov_len: b.len(),
            })
            .collect::<Vec<libc::iovec>>();

        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!(
            "registering writev op buffer len: {}, file offset: {}",
            iobufs.len(),
            file_offset
        );

        let bufsptr = &*iobufs;

        let sqe = opcode::Writev::new(types::Fd(file.as_raw_fd()), bufsptr.as_ptr(), 1)
            .offset(file_offset)
            .build();

        // transmute iobufs into a Box<[u8]> to keep it alive during the operation
        let iobufs = unsafe {
            let len = iobufs.len() * std::mem::size_of::<libc::iovec>();
            let ptr = Box::into_raw(iobufs.into_boxed_slice()) as *mut u8;
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len))
        };

        // SAFETY: `bufs` are owned by this operation, as well as its iovecs
        unsafe {
            Op::new(
                sqe,
                WriteVectored {
                    buf: bufs,
                    iovecs: iobufs,
                    file,
                },
            )
        }
    }
}

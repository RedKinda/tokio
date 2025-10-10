use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::net::TcpListener;
use std::os::unix::io::{AsRawFd, RawFd};
use std::{io, ptr};

use crate::io::unix::AsyncFd;
use crate::sync::oneshot;
use crate::task::LocalSet;
use io_uring::{IoUring, SubmissionQueue, opcode, squeue, types};
use slab::Slab;

#[derive(Clone, Debug)]
enum Token {
    Accept,
    Poll {
        fd: RawFd,
    },
    Read {
        fd: RawFd,
        buf_index: usize,
    },
    Write {
        fd: RawFd,
        buf_index: usize,
        offset: usize,
        len: usize,
    },
}

pub struct AcceptCount {
    entry: squeue::Entry,
    count: usize,
}

impl AcceptCount {
    fn new(fd: RawFd, token: usize, count: usize) -> AcceptCount {
        AcceptCount {
            entry: opcode::Accept::new(types::Fd(fd), ptr::null_mut(), ptr::null_mut())
                .build()
                .user_data(token as _),
            count,
        }
    }

    pub fn push_to(&mut self, sq: &mut SubmissionQueue<'_>) {
        while self.count > 0 {
            unsafe {
                match sq.push(&self.entry) {
                    Ok(_) => self.count -= 1,
                    Err(_) => break,
                }
            }
        }

        sq.sync();
    }
}
thread_local! {
    static RING: RefCell<LocalUring> = RefCell::new(LocalUring::new());
}

fn with_ring<F, R>(f: F) -> R
where
    F: FnOnce(&mut LocalUring) -> R,
{
    RING.with(|ring| f(&mut ring.borrow_mut()))
}

struct UringSocket {
    fd: RawFd,
    slab_slot: Option<usize>,
}

type ResponseType = Result<PendingRequestResponse, io::Error>;

impl UringSocket {
    fn submit_request(&mut self, req: PendingRequestType) -> oneshot::Receiver<ResponseType> {
        with_ring(|ring| {
            let (rx, slab_slot) = ring.submit_request(req, self.slab_slot);
            self.slab_slot = Some(slab_slot);
            rx
        })
    }

    fn write(&mut self, buf: Vec<u8>) -> impl Future<Output = Result<usize, io::Error>> + use<> {
        let rx = self.submit_request(PendingRequestType::Write { fd: self.fd, buf });

        async {
            match rx.await.unwrap()? {
                PendingRequestResponse::WriteResponse(len) => Ok(len),
                _ => unreachable!(),
            }
        }
    }

    fn recv(&mut self, buf: Vec<u8>) -> impl Future<Output = Result<(Vec<u8>, usize), io::Error>> {
        let rx = self.submit_request(PendingRequestType::Recv { fd: self.fd, buf });

        async {
            match rx.await.unwrap()? {
                PendingRequestResponse::ReadResponse(buf, len) => Ok((buf, len)),
                _ => unreachable!(),
            }
        }
    }

    fn accept(listener: &TcpListener) -> impl Future<Output = io::Result<UringSocket>> {
        let fd = listener.as_raw_fd();
        let rx = with_ring(|ring| {
            let (rx, _) = ring.submit_request(PendingRequestType::Accept { fd }, None);
            rx
        });

        async move {
            match rx.await.unwrap()? {
                PendingRequestResponse::AcceptResponse { fd, slab_slot } => Ok(UringSocket {
                    fd,
                    slab_slot: Some(slab_slot),
                }),
                _ => unreachable!(),
            }
        }
    }
}

enum PendingRequestType {
    Nop,
    Accept { fd: RawFd },
    Write { fd: RawFd, buf: Vec<u8> },
    Recv { fd: RawFd, buf: Vec<u8> },
}

impl PendingRequestType {
    fn into_opcode(&self) -> squeue::Entry {
        match self {
            PendingRequestType::Nop => opcode::Nop::new().build(),
            PendingRequestType::Accept { fd } => {
                opcode::Accept::new(types::Fd(*fd), ptr::null_mut(), ptr::null_mut()).build()
            }
            PendingRequestType::Write { fd, buf } => {
                opcode::Write::new(types::Fd(*fd), buf.as_ptr(), buf.len() as _).build()
            }
            PendingRequestType::Recv { fd, buf } => {
                opcode::Recv::new(types::Fd(*fd), buf.as_ptr() as *mut u8, buf.len() as _).build()
            }
        }
    }
}

enum PendingRequestResponse {
    None,
    WriteResponse(usize),
    ReadResponse(Vec<u8>, usize),
    AcceptResponse { fd: RawFd, slab_slot: usize },
}

struct PendingRequest {
    sender: oneshot::Sender<ResponseType>,
    req_type: PendingRequestType,
}

pub struct LazyLocalUring(Option<LocalUring>);
impl LazyLocalUring {
    pub const fn new() -> Self {
        Self(None)
    }

    fn get_or_init(&mut self) -> &mut LocalUring {
        if self.0.is_none() {
            self.0 = Some(LocalUring::new());
        }
        self.0.as_mut().unwrap()
    }
}

struct LocalUring {
    ring: IoUring,
    pending_requests: Slab<PendingRequest>,
    backlog: VecDeque<squeue::Entry>,
}

impl LocalUring {
    fn new() -> Self {
        let n = Self {
            ring: IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
                .build(256)
                .unwrap(),
            pending_requests: Slab::with_capacity(256),
            backlog: VecDeque::new(),
        };

        let eventfd = unsafe { libc::eventfd(0, 0) };

        n.ring.submitter().register_eventfd(eventfd).unwrap();

        let f = AsyncFd::new(eventfd).unwrap();

        let local_set = LocalSet::new();

        n

        // local_set.spawn_local(future)
    }

    fn flush_backlog(&mut self) {
        while let Some(backlogged) = self.backlog.pop_front() {
            let r = unsafe { self.ring.submission().push(&backlogged) };
            if r.is_err() {
                self.backlog.push_front(backlogged);
                break;
            }
        }

        self.ring.submit();
        self.ring.submission().sync();
    }

    fn submit_request(
        &mut self,
        req: PendingRequestType,
        slab_slot: Option<usize>,
    ) -> (oneshot::Receiver<ResponseType>, usize) {
        let (sender, receiver) = oneshot::channel();
        let actual_slab_slot;

        let opcode = req.into_opcode();

        let request = PendingRequest {
            sender,
            req_type: req,
        };

        if let Some(slot) = slab_slot {
            actual_slab_slot = slot;
            self.pending_requests[slot] = request;
        } else {
            let vacant = self.pending_requests.vacant_entry();
            actual_slab_slot = vacant.key();
            vacant.insert(request);
        };

        let opcode = opcode.user_data(actual_slab_slot as _);

        let result = unsafe { self.ring.submission().push(&opcode) };

        if result.is_err() {
            self.backlog.push_back(opcode);
        } else {
            self.ring.submission().sync();
        }

        self.ring.submit();
        self.read_cq();
        (receiver, actual_slab_slot)
    }

    fn read_cq(&mut self) {
        self.ring.completion().sync();

        for cqe in &mut self.ring.completion() {
            let res = cqe.result();
            let slab_slot = cqe.user_data() as usize;

            let pending = self.pending_requests.remove(slab_slot);
            let response = if res < 0 {
                Err(io::Error::from_raw_os_error(-res))
            } else {
                match pending.req_type {
                    PendingRequestType::Nop => Ok(PendingRequestResponse::None),
                    PendingRequestType::Accept { .. } => {
                        Ok(PendingRequestResponse::AcceptResponse { fd: res, slab_slot })
                    }
                    PendingRequestType::Write { buf, .. } => {
                        Ok(PendingRequestResponse::WriteResponse(res as usize))
                    }
                    PendingRequestType::Recv { buf, .. } => {
                        Ok(PendingRequestResponse::ReadResponse(buf, res as usize))
                    }
                }
            };

            let _ = pending.sender.send(response);
        }
    }
}

pub async fn my_main() -> anyhow::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", 3456))?;
    println!("Listening on {}", listener.local_addr()?);

    loop {
        let mut s = UringSocket::accept(&listener).await?;
        println!("Accepted connection: fd={}", s.fd);

        let buf = vec![0u8; 1024];
        let r = s.recv(buf).await;
        let (buf, len) = r?;
        println!("Received: {:?}", std::str::from_utf8(&buf[..len]));
        s.write(buf[..len].to_vec()).await?;
    }
}

pub fn main_example() -> anyhow::Result<()> {
    let mut ring = IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
        .build(256)
        .unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 3456))?;

    let mut backlog = VecDeque::new();
    let mut bufpool = Vec::with_capacity(64);
    let mut buf_alloc = Slab::with_capacity(64);
    let mut token_alloc = Slab::with_capacity(64);

    println!("listen {}", listener.local_addr()?);

    let (submitter, mut sq, mut cq) = ring.split();

    let mut accept = AcceptCount::new(listener.as_raw_fd(), token_alloc.insert(Token::Accept), 3);

    accept.push_to(&mut sq);

    loop {
        match submitter.submit_and_wait(1) {
            Ok(_) => (),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => (),
            Err(err) => return Err(err.into()),
        }
        cq.sync();

        // clean backlog
        loop {
            if sq.is_full() {
                match submitter.submit() {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => break,
                    Err(err) => return Err(err.into()),
                }
            }
            sq.sync();

            match backlog.pop_front() {
                Some(sqe) => unsafe {
                    let _ = sq.push(&sqe);
                },
                None => break,
            }
        }

        accept.push_to(&mut sq);

        for cqe in &mut cq {
            let ret = cqe.result();
            let token_index = cqe.user_data() as usize;

            if ret < 0 {
                eprintln!(
                    "token {:?} error: {:?}",
                    token_alloc.get(token_index),
                    io::Error::from_raw_os_error(-ret)
                );
                continue;
            }

            let token = &mut token_alloc[token_index];
            match token.clone() {
                Token::Accept => {
                    println!("accept");

                    accept.count += 1;

                    let fd = ret;
                    let poll_token = token_alloc.insert(Token::Poll { fd });

                    let poll_e = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as _)
                        .build()
                        .user_data(poll_token as _);

                    unsafe {
                        if sq.push(&poll_e).is_err() {
                            backlog.push_back(poll_e);
                        }
                    }
                }
                Token::Poll { fd } => {
                    let (buf_index, buf) = match bufpool.pop() {
                        Some(buf_index) => (buf_index, &mut buf_alloc[buf_index]),
                        None => {
                            let buf = vec![0u8; 2048].into_boxed_slice();
                            let buf_entry = buf_alloc.vacant_entry();
                            let buf_index = buf_entry.key();
                            (buf_index, buf_entry.insert(buf))
                        }
                    };

                    *token = Token::Read { fd, buf_index };

                    let read_e = opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
                        .build()
                        .user_data(token_index as _);

                    unsafe {
                        if sq.push(&read_e).is_err() {
                            backlog.push_back(read_e);
                        }
                    }
                }
                Token::Read { fd, buf_index } => {
                    if ret == 0 {
                        bufpool.push(buf_index);
                        token_alloc.remove(token_index);

                        println!("shutdown");

                        unsafe {
                            libc::close(fd);
                        }
                    } else {
                        let len = ret as usize;
                        let buf = &buf_alloc[buf_index];

                        *token = Token::Write {
                            fd,
                            buf_index,
                            len,
                            offset: 0,
                        };

                        let write_e = opcode::Send::new(types::Fd(fd), buf.as_ptr(), len as _)
                            .build()
                            .user_data(token_index as _);

                        unsafe {
                            if sq.push(&write_e).is_err() {
                                backlog.push_back(write_e);
                            }
                        }
                    }
                }
                Token::Write {
                    fd,
                    buf_index,
                    offset,
                    len,
                } => {
                    let write_len = ret as usize;

                    let entry = if offset + write_len >= len {
                        bufpool.push(buf_index);

                        *token = Token::Poll { fd };

                        opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as _)
                            .build()
                            .user_data(token_index as _)
                    } else {
                        let offset = offset + write_len;
                        let len = len - offset;

                        let buf = &buf_alloc[buf_index][offset..];

                        *token = Token::Write {
                            fd,
                            buf_index,
                            offset,
                            len,
                        };

                        opcode::Write::new(types::Fd(fd), buf.as_ptr(), len as _)
                            .build()
                            .user_data(token_index as _)
                    };

                    unsafe {
                        if sq.push(&entry).is_err() {
                            backlog.push_back(entry);
                        }
                    }
                }
            }
        }
    }
}

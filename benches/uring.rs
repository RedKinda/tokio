#![cfg(unix)]

use tokio::net::unix::pipe::{self, UringReceiver, UringSender, make_uring_pipe};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
use tokio_util::codec::{BytesCodec, FramedRead /*FramedWrite*/};

use criterion::{Criterion, criterion_group, criterion_main};

use std::fs::File as StdFile;
use std::io::Read as StdRead;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::thread::{self};

fn rt() -> tokio::runtime::Runtime {
    let mut r = tokio::runtime::Builder::new_multi_thread();

    #[cfg(all(tokio_unstable, feature = "io-uring", target_os = "linux"))]
    let r = r.enable_io_uring();

    r.enable_all().build().unwrap()
}

#[cfg(not(all(tokio_unstable, feature = "io-uring", target_os = "linux")))]
type RxType = tokio::net::unix::pipe::Receiver;
#[cfg(all(tokio_unstable, feature = "io-uring", target_os = "linux"))]
type RxType = UringReceiver;

#[cfg(not(all(tokio_unstable, feature = "io-uring", target_os = "linux")))]
type TxType = tokio::net::unix::pipe::Sender;
#[cfg(all(tokio_unstable, feature = "io-uring", target_os = "linux"))]
type TxType = UringSender;

fn make_pipe() -> (TxType, RxType) {
    #[cfg(all(tokio_unstable, feature = "io-uring", target_os = "linux"))]
    return make_uring_pipe().unwrap();
    #[cfg(not(all(tokio_unstable, feature = "io-uring", target_os = "linux")))]
    return pipe::pipe().unwrap();
}

fn async_one_pipe(c: &mut Criterion) {
    let rt = rt();

    c.bench_function("async_one_pipe", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let (mut tx, mut rx) = make_pipe();

                let mut tx_buf = vec![1u8; 50 * 1024];
                let mut rx_buf = vec![0u8; 50 * 1024];

                let now = std::time::Instant::now();
                for _ in 0..iters {
                    #[cfg(not(all(tokio_unstable, feature = "io-uring", target_os = "linux")))]
                    {
                        tx.write_all(&tx_buf).await.unwrap();
                        rx.read_exact(&mut rx_buf).await.unwrap();
                    }

                    #[cfg(all(tokio_unstable, feature = "io-uring", target_os = "linux"))]
                    {
                        let (res, _buf) = tx.write_all(tx_buf).await;
                        res.unwrap();
                        tx_buf = _buf;
                        rx_buf.truncate(0);
                        let (res, _buf) = rx.read_all(rx_buf).await;
                        res.unwrap();
                        rx_buf = _buf;
                    }
                }

                now.elapsed()
            })
        });
    });
}

fn stress_pipe_split(
    mut tx: TxType,
    mut rx: RxType,
    mut tx_buf: Vec<u8>,
    mut rx_buf: Vec<u8>,
    iters: u64,
    send_count: u64,
) -> JoinHandle<()> {
    let read_task = tokio::spawn(async move {
        for _ in 0..iters {
            for _ in 0..send_count {
                #[cfg(not(all(tokio_unstable, feature = "io-uring", target_os = "linux")))]
                {
                    rx.read_exact(&mut rx_buf).await.unwrap();
                }

                #[cfg(all(tokio_unstable, feature = "io-uring", target_os = "linux"))]
                {
                    rx_buf.truncate(0);
                    // panic!("now reading");
                    let (res, _buf) = rx.read_all(rx_buf).await;
                    res.unwrap();
                    rx_buf = _buf;
                }
            }
        }
    });

    let write_task = tokio::spawn(async move {
        for _ in 0..iters {
            for _ in 0..send_count {
                #[cfg(not(all(tokio_unstable, feature = "io-uring", target_os = "linux")))]
                {
                    tx.write_all(&tx_buf).await.unwrap();
                }

                #[cfg(all(tokio_unstable, feature = "io-uring", target_os = "linux"))]
                {
                    let (res, _buf) = tx.write_all(tx_buf).await;
                    res.unwrap();
                    tx_buf = _buf;
                }
            }
        }
    });

    tokio::spawn(async move {
        write_task.await.unwrap();
        read_task.await.unwrap();
    })
}

fn async_one_pipe_split(c: &mut Criterion) {
    let rt = rt();

    c.bench_function("async_one_pipe_split", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let (tx, rx) = make_pipe();

                let tx_buf = vec![1u8; 50 * 1024];
                let rx_buf = vec![0u8; 50 * 1024];

                let send_count = 10;

                let now = std::time::Instant::now();

                stress_pipe_split(tx, rx, tx_buf, rx_buf, iters, send_count)
                    .await
                    .unwrap();

                now.elapsed()
            })
        });
    });
}

fn async_many_pipes_split(c: &mut Criterion) {
    let rt = rt();

    c.bench_function("async_many_pipes_split", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let pipe_count = 64;
                let send_count = 10;
                let mut handles: Vec<JoinHandle<()>> = Vec::new();

                let now = std::time::Instant::now();

                for _ in 0..pipe_count {
                    let (tx, rx) = make_pipe();
                    let tx_buf = vec![1u8; 50 * 1024];
                    let rx_buf = vec![0u8; 50 * 1024];

                    handles.push(stress_pipe_split(tx, rx, tx_buf, rx_buf, iters, send_count));
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                now.elapsed()
            })
        });
    });
}

criterion_group!(
    file,
    async_one_pipe,
    async_one_pipe_split,
    async_many_pipes_split
);
criterion_main!(file);

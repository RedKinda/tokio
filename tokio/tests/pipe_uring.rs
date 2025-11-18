//! Uring file operations tests.

#![cfg(all(
    tokio_unstable,
    feature = "io-uring",
    feature = "rt",
    feature = "fs",
    target_os = "linux"
))]

use futures::future::FutureExt;
use std::future::Future;
use std::io::SeekFrom;
use std::sync::mpsc;
use std::task::Poll;
use std::time::Duration;
use std::{future::poll_fn, path::PathBuf};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::io::{AsyncSeekExt as _, AsyncWriteExt as _};
use tokio::net::unix::pipe::{make_uring_pipe, UringReceiver, UringSender};
use tokio::task::JoinHandle;
use tokio::{
    fs::OpenOptions,
    runtime::{Builder, Runtime},
};
use tokio_test::assert_ok;
use tokio_util::task::TaskTracker;

fn multi_rt(n: usize) -> Box<dyn Fn() -> Runtime> {
    Box::new(move || {
        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!("building multi-threaded rt with {n} threads");
        Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .enable_io_uring()
            .build()
            .unwrap()
    })
}

fn current_rt() -> Box<dyn Fn() -> Runtime> {
    Box::new(|| {
        #[cfg(all(tokio_unstable, feature = "tracing"))]
        tracing::trace!("building current-thread rt");
        Builder::new_current_thread()
            .enable_all()
            .enable_io_uring()
            .build()
            .unwrap()
    })
}

fn rt_combinations() -> Vec<Box<dyn Fn() -> Runtime>> {
    vec![
        current_rt(),
        multi_rt(1),
        multi_rt(4),
        multi_rt(16),
        // multi_rt(64),
        // multi_rt(256),
    ]
}

fn with_rt_combinations<R: Future + Send + 'static, F: Fn() -> R + 'static + Sync>(f: &'static F)
where
    R::Output: Send,
{
    // tracing_subscriber::fmt()
    // .with_max_level(tracing::Level::TRACE)
    // .with_thread_ids(true)
    // .with_thread_names(true)
    // // .with_ansi(false)
    // .try_init();

    let test_name = std::thread::current()
        .name()
        .map(|n| n.to_string())
        .unwrap();

    println!("running test: {test_name}");

    for rt in rt_combinations() {
        rt().block_on(async {
            // tokio::spawn(async {
            //     f();
            // })
            // .await
            // .unwrap();

            f().await;
        });
    }
}

#[test]
fn test_uring_pipe() {
    with_rt_combinations(&|| async {
        let (tx, rx) = make_uring_pipe().unwrap();

        // pipes usually have a capacity of 64KiB on Linux
        // if we try to write more, and block, the test will deadlock
        let mut tx_buf = vec![1u8; 50 * 1024];
        let mut rx_buf = vec![0u8; 50 * 1024];

        for _ in 0..32 {
            let (res, _buf) = tx.write_all(tx_buf).await;
            let res = res.unwrap();
            tx_buf = _buf;
            rx_buf.truncate(0);
            let (res, _buf) = rx.read_all(rx_buf).await;
            res.unwrap();
            rx_buf = _buf;
            assert_eq!(rx_buf, [1u8; 50 * 1024])
        }
    });
}

fn stress_pipe_split(
    mut tx: UringSender,
    mut rx: UringReceiver,
    mut tx_buf: Vec<u8>,
    mut rx_buf: Vec<u8>,
    iters: u64,
) -> JoinHandle<()> {
    let read_task = tokio::spawn(async move {
        for _ in 0..iters {
            rx_buf.truncate(0);
            // panic!("now reading");
            let (res, _buf) = rx.read_all(rx_buf).await;
            res.unwrap();
            rx_buf = _buf;
        }
    });

    let write_task = tokio::spawn(async move {
        for _ in 0..iters {
            let (res, _buf) = tx.write_all(tx_buf).await;
            res.unwrap();
            tx_buf = _buf;
        }
    });

    tokio::spawn(async move {
        write_task.await.unwrap();
        read_task.await.unwrap();
    })
}

#[test]
fn test_single_pipe() {
    with_rt_combinations(&|| async {
        let (tx, rx) = make_uring_pipe().unwrap();

        let tx_buf = vec![1u8; 100 * 1024];
        let rx_buf = vec![0u8; 100 * 1024];

        let iters = 100;

        stress_pipe_split(tx, rx, tx_buf, rx_buf, iters)
            .await
            .unwrap();
    });
}

#[test]
fn test_multi_pipe() {
    with_rt_combinations(&|| async {
        let tx_buf = vec![1u8; 100 * 1024];
        let rx_buf = vec![0u8; 100 * 1024];

        let iters = 100;

        let mut handles = Vec::new();
        for _ in 0..32 {
            let (tx, rx) = make_uring_pipe().unwrap();
            handles.push(stress_pipe_split(
                tx,
                rx,
                tx_buf.clone(),
                rx_buf.clone(),
                iters,
            ));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    });
}

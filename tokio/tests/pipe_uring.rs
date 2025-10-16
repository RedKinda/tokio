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
use tokio::net::unix::pipe::make_uring_pipe;
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
        multi_rt(2),
        multi_rt(8),
        // multi_rt(64),
        // multi_rt(256),
    ]
}

fn with_rt_combinations<R: Future + Send + 'static, F: Fn() -> R + 'static + Sync>(f: &'static F)
where
    R::Output: Send,
{
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_ansi(false)
        .try_init();

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

        let mut tx_buf = vec![1u8; 1024];
        let mut rx_buf = vec![0u8; 1024];

        for _ in 0..32 {
            let (res, _buf) = tx.write_all(tx_buf).await;
            let res = res.unwrap();
            tx_buf = _buf;
            println!("written {res} bytes");
            rx_buf.truncate(0);
            let (res, _buf) = rx.read_all(rx_buf).await;
            res.unwrap();
            rx_buf = _buf;
            assert_eq!(rx_buf, [1u8; 1024])
        }
    });
}

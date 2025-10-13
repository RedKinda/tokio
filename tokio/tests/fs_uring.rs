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
use tokio::{
    fs::OpenOptions,
    runtime::{Builder, Runtime},
};
use tokio_test::assert_ok;
use tokio_util::task::TaskTracker;

fn multi_rt(n: usize) -> Box<dyn Fn() -> Runtime> {
    Box::new(move || {
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
        multi_rt(64),
        multi_rt(256),
    ]
}

fn with_rt_combinations<R: Future + Send + 'static, F: Fn() -> R + 'static + Sync>(f: &'static F)
where
    R::Output: Send,
{
    let test_name = std::thread::current()
        .name()
        .map(|n| n.to_string())
        .unwrap();

    println!("running test: {test_name}");

    for rt in rt_combinations() {
        rt().block_on(async {
            tokio::spawn(async {
                f();
            })
            .await
            .unwrap();
        });
    }
}

#[test]
fn shutdown_runtime_while_performing_io_uring_ops() {
    fn run(rt: Runtime) {
        let (tx, rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let (_tmp, path) = create_tmp_files(1);
        rt.spawn(async move {
            let path = path[0].clone();

            // spawning a bunch of uring operations.
            loop {
                let path = path.clone();
                tokio::spawn(async move {
                    let mut opt = OpenOptions::new();
                    opt.read(true);
                    opt.open(&path).await.unwrap();
                });

                // Avoid busy looping.
                tokio::task::yield_now().await;
            }
        });

        std::thread::spawn(move || {
            let rt: Runtime = rx.recv().unwrap();
            rt.shutdown_timeout(Duration::from_millis(300));
            done_tx.send(()).unwrap();
        });

        tx.send(rt).unwrap();
        done_rx.recv().unwrap();
    }

    for rt in rt_combinations() {
        run(rt());
    }
}

#[test]
fn open_many_files() {
    with_rt_combinations(&|| async {
        const NUM_FILES: usize = 512;

        let (_tmp_files, paths): (Vec<NamedTempFile>, Vec<PathBuf>) = create_tmp_files(NUM_FILES);
        let tracker = TaskTracker::new();

        for i in 0..10_000 {
            let path = paths.get(i % NUM_FILES).unwrap().clone();
            tracker.spawn(async move {
                let _file = OpenOptions::new().read(true).open(path).await.unwrap();
            });
        }
        tracker.close();
        tracker.wait().await;
    });
}

#[tokio::test]
async fn cancel_op_future() {
    let (_tmp_file, path): (Vec<NamedTempFile>, Vec<PathBuf>) = create_tmp_files(1);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        poll_fn(|cx| {
            let opt = {
                let mut opt = tokio::fs::OpenOptions::new();
                opt.read(true);
                opt
            };

            let fut = opt.open(&path[0]);

            // If io_uring is enabled (and not falling back to the thread pool),
            // the first poll should return Pending.
            let _pending = Box::pin(fut).poll_unpin(cx);

            tx.send(()).unwrap();

            Poll::<()>::Pending
        })
        .await;
    });

    // Wait for the first poll
    rx.recv().await.unwrap();

    handle.abort();

    let res = handle.await.unwrap_err();
    assert!(res.is_cancelled());
}

#[test]
fn path_read_write_uring() {
    // trace level logging is useful for debugging
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::TRACE)
    //     .with_thread_ids(true)
    //     .with_thread_names(true)
    //     .with_ansi(false)
    //     .init();

    with_rt_combinations(&|| async {
        assert_ok!(fs::write(&create_tmp_files(1).1[0], b"bytes").await);
    });
}

#[test]
fn path_write_concurrent() {
    with_rt_combinations(&|| async {
        // sleep 0.5s
        tokio::time::sleep(Duration::from_secs(5)).await;

        // spawn 1024 tasks that write to separate files
        let temp = tempdir();
        let dir = temp.path();

        const NUM_TASKS: usize = 4;
        const NUM_FILES_PER_TASK: usize = 4;

        let mut handles = Vec::with_capacity(NUM_TASKS);
        for i in 0..NUM_TASKS {
            let path = dir.join(format!("file-{}", i));
            handles.push(tokio::spawn(async move {
                for j in 0..NUM_FILES_PER_TASK {
                    let file_path = path.with_file_name(format!("file-{}-{}", i, j));
                    assert_ok!(fs::write(&file_path, b"bytes").await);
                    // sleep i ms
                    tokio::time::sleep(std::time::Duration::from_millis(i as u64)).await;
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // verify that all files were written
        for i in 0..NUM_TASKS {
            for j in 0..NUM_FILES_PER_TASK {
                let file_path = dir.join(format!("file-{}-{}", i, j));
                let out = assert_ok!(fs::read(&file_path).await);
                assert_eq!(out, b"bytes");
            }
        }
    });
}

#[test]
fn path_write_massive() {
    with_rt_combinations(&|| async {
        // write a 2.5gb file - this is bigger than a single write uring can handle
        let path = &create_tmp_files(1).1[0];

        const SIZE: usize = 2_500_000_000;
        let data = vec![1u8; SIZE];
        assert_ok!(fs::write(path, &data).await);
        drop(data);

        let out = assert_ok!(fs::read(path).await);
        assert_eq!(out.len(), SIZE);
    });
}

#[test]
fn test_async_write() {
    with_rt_combinations(&|| async {
        let path = &create_tmp_files(1).1[0];

        let mut file = assert_ok!(fs::File::create(path).await);

        assert_ok!(file.write_all(b"hello world").await);

        // write some more
        assert_ok!(file.write_all(b"!!!").await);

        assert_ok!(file.flush().await);

        let out = assert_ok!(fs::read(path).await);
        assert_eq!(out, b"hello world!!!");

        file.seek(SeekFrom::Start(0)).await.unwrap();
        file.write_all(b"meow!").await.unwrap();
        file.flush().await.unwrap();

        let out = assert_ok!(fs::read(path).await);
        assert_eq!(out, b"meow! world!!!");
    });
}

#[test]
fn test_cancelled_write() {
    with_rt_combinations(&|| async {
        let path = &create_tmp_files(1).1[0];

        let mut file = assert_ok!(fs::File::create(path).await);

        let mut write_fut = Box::pin(file.write(b"hello world!!!"));
        poll_fn(move |cx| {
            assert!(write_fut.as_mut().poll(cx).is_ready());

            Poll::Ready(())
        })
        .await;
    });
}

fn create_tmp_files(num_files: usize) -> (Vec<NamedTempFile>, Vec<PathBuf>) {
    let mut files = Vec::with_capacity(num_files);
    for _ in 0..num_files {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        files.push((tmp, path));
    }

    files.into_iter().unzip()
}

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

#![warn(rust_2018_idioms)]
#![cfg(all(feature = "full", not(target_os = "wasi")))] // Wasi does not support file operations

use futures::Future;
use std::{future::poll_fn, io::SeekFrom, task::Poll};

use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt as _},
};
use tokio_test::assert_ok;

#[tokio::test]
async fn path_read_write() {
    let temp = tempdir();
    let dir = temp.path();

    assert_ok!(fs::write(dir.join("bar"), b"bytes").await);
    let out = assert_ok!(fs::read(dir.join("bar")).await);

    assert_eq!(out, b"bytes");
}

#[tokio::test]
async fn path_write_concurrent() {
    // spawn 1024 tasks that write to separate files
    let temp = tempdir();
    let dir = temp.path();

    const NUM_TASKS: usize = 256;
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
}

#[tokio::test]
async fn path_write_massive() {
    // write a 2.5gb file
    let temp = tempdir();
    let dir = temp.path();

    const SIZE: usize = 2_500_000_000;
    let data = vec![1u8; SIZE];
    assert_ok!(fs::write(dir.join("bigfile"), &data).await);
    drop(data);

    let out = assert_ok!(fs::read(dir.join("bigfile")).await);
    assert_eq!(out.len(), SIZE);
}

#[tokio::test]
async fn test_async_write() {
    let temp = tempdir();
    let dir = temp.path();

    let mut file = assert_ok!(fs::File::create(dir.join("foo")).await);

    assert_ok!(file.write_all(b"hello world").await);

    // write some more
    assert_ok!(file.write_all(b"!!!").await);

    assert_ok!(file.flush().await);

    let out = assert_ok!(fs::read(dir.join("foo")).await);
    assert_eq!(out, b"hello world!!!");

    file.seek(SeekFrom::Start(0)).await.unwrap();
    file.write_all(b"meow!").await.unwrap();
    file.flush().await.unwrap();

    let out = assert_ok!(fs::read(dir.join("foo")).await);
    assert_eq!(out, b"meow! world!!!");
}

#[tokio::test]
async fn test_cancelled_write() {
    let temp = tempdir();
    let dir = temp.path();

    let mut file = assert_ok!(fs::File::create(dir.join("foo")).await);

    let mut write_fut = Box::pin(file.write(b"hello world!!!"));
    poll_fn(move |cx| {
        assert!(write_fut.as_mut().poll(cx).is_ready());

        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn try_clone_should_preserve_max_buf_size() {
    let buf_size = 128;
    let temp = tempdir();
    let dir = temp.path();

    let mut file = fs::File::create(dir.join("try_clone_should_preserve_max_buf_size"))
        .await
        .unwrap();
    file.set_max_buf_size(buf_size);

    let cloned = file.try_clone().await.unwrap();

    assert_eq!(cloned.max_buf_size(), buf_size);
}

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

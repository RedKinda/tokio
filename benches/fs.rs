#![cfg(unix)]

use tokio_stream::StreamExt;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
use tokio_util::codec::{BytesCodec, FramedRead /*FramedWrite*/};

use criterion::{Criterion, criterion_group, criterion_main};

use std::fs::File as StdFile;
use std::io::Read as StdRead;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(2)
        .build()
        .unwrap()
}

const BLOCK_COUNT: usize = 1_000;

const BUFFER_SIZE: usize = 4096;
const DEV_ZERO: &str = "/dev/zero";

fn async_read_codec(c: &mut Criterion) {
    let rt = rt();

    c.bench_function("async_read_codec", |b| {
        b.iter(|| {
            let task = || async {
                let file = File::open(DEV_ZERO).await.unwrap();
                let mut input_stream =
                    FramedRead::with_capacity(file, BytesCodec::new(), BUFFER_SIZE);

                for _i in 0..BLOCK_COUNT {
                    let _bytes = input_stream.next().await.unwrap();
                }
            };

            rt.block_on(task());
        })
    });
}

fn async_read_buf(c: &mut Criterion) {
    let rt = rt();

    c.bench_function("async_read_buf", |b| {
        b.iter(|| {
            let task = || async {
                let mut file = File::open(DEV_ZERO).await.unwrap();
                let mut buffer = [0u8; BUFFER_SIZE];

                for _i in 0..BLOCK_COUNT {
                    let count = file.read(&mut buffer).await.unwrap();
                    if count == 0 {
                        break;
                    }
                }
            };

            rt.block_on(task());
        });
    });
}

fn async_read_std_file(c: &mut Criterion) {
    let rt = rt();

    c.bench_function("async_read_std_file", |b| {
        b.iter(|| {
            let task = || async {
                let mut file =
                    tokio::task::block_in_place(|| Box::pin(StdFile::open(DEV_ZERO).unwrap()));

                for _i in 0..BLOCK_COUNT {
                    let mut buffer = [0u8; BUFFER_SIZE];
                    let mut file_ref = file.as_mut();

                    tokio::task::block_in_place(move || {
                        file_ref.read_exact(&mut buffer).unwrap();
                    });
                }
            };

            rt.block_on(task());
        });
    });
}

fn sync_read(c: &mut Criterion) {
    c.bench_function("sync_read", |b| {
        b.iter(|| {
            let mut file = StdFile::open(DEV_ZERO).unwrap();
            let mut buffer = [0u8; BUFFER_SIZE];

            for _i in 0..BLOCK_COUNT {
                file.read_exact(&mut buffer).unwrap();
            }
        })
    });
}

fn async_write_one(c: &mut Criterion) {
    let rt = rt();

    const WRITE_SIZE: usize = 10 * 1024;
    let data: &'static mut [u8] = Box::leak(vec![0u8; WRITE_SIZE].into_boxed_slice());

    c.bench_function("async_write_one", |b| {
        b.iter(|| {
            let task = || async {
                let mut file = File::open("/dev/null").await.unwrap();
                file.write_all(data).await.unwrap();
            };

            rt.block_on(task());
        });
    });
}

fn async_write_a_lot(c: &mut Criterion) {
    let rt = rt();

    const WRITE_SIZE: usize = 10 * 1024;
    let data: &'static mut [u8] = Box::leak(vec![0u8; WRITE_SIZE].into_boxed_slice());

    c.bench_function("async_write_a_lot", |b| {
        b.iter(|| {
            let task_inner = || async {
                let mut file = File::open("/dev/null").await.unwrap();
                file.write_all(data).await.unwrap();
            };

            let task = || async {
                let mut tasks = vec![];
                for _ in 0..1 {
                    tasks.push(tokio::spawn(async move {
                        task_inner().await;
                    }));
                }
                for task in tasks {
                    task.await.unwrap();
                }
            };

            rt.block_on(task());
        });
    });
}

criterion_group!(
    file,
    async_read_std_file,
    async_read_buf,
    async_read_codec,
    sync_read,
    async_write_one,
    async_write_a_lot
);
criterion_main!(file);

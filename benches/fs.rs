#![cfg(unix)]

use tokio_stream::StreamExt;

use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
use tokio_util::codec::{BytesCodec, FramedRead /*FramedWrite*/};

use criterion::{Criterion, criterion_group, criterion_main};

use std::fs::File as StdFile;
use std::io::Read as StdRead;

fn rt() -> tokio::runtime::Runtime {
    let mut r = tokio::runtime::Builder::new_multi_thread();

    #[cfg(all(tokio_unstable, feature = "io-uring", target_os = "linux"))]
    let r = r.enable_io_uring();

    r.enable_all().build().unwrap()
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
        std::fs::create_dir_all("./tmp").unwrap();

        b.iter(|| {
            let task = || async {
                // let mut file = File::open("/dev/null").await.unwrap();
                // file.write_all(data).await.unwrap();
                fs::write("./tmp/onefile", &data).await.unwrap();
            };

            rt.block_on(task());
        });

        // purge the folder
        std::fs::remove_dir_all("./tmp").unwrap();
    });
}

fn async_write_a_lot(c: &mut Criterion) {
    let rt = rt();

    const WRITE_SIZE: usize = 10 * 1024;
    const WRITE_COUNT: usize = 32;
    let data: &'static mut [u8] = Box::leak(vec![0u8; WRITE_SIZE].into_boxed_slice());

    c.bench_function("async_write_a_lot", |b| {
        b.iter_custom(|iters| {
            let task_inner = |taskid: usize| {
                let data: &'static [u8] = data;
                async move {
                    // let mut file = File::options()
                    //     .append(true)
                    //     .open("/dev/null")
                    //     .await
                    //     .unwrap();

                    // for i in 0..WRITE_COUNT {
                    //     file.write_all(data).await.unwrap();
                    // }

                    for i in 0..WRITE_COUNT {
                        fs::write(format!("./tmp/filelot{taskid}-{i}"), &data)
                            .await
                            .unwrap();
                    }
                }
            };

            let task = || async {
                for _iter in 0..iters {
                    let mut tasks = vec![];
                    for taskid in 0..32 {
                        tasks.push(tokio::spawn(async move {
                            task_inner(taskid).await;
                        }));
                    }
                    for task in tasks {
                        task.await.unwrap();
                    }
                }
            };

            // make the tmp folder
            std::fs::create_dir_all("./tmp").unwrap();

            let now = std::time::Instant::now();
            rt.block_on(task());
            let elapsed = now.elapsed();

            // purge the folder
            std::fs::remove_dir_all("./tmp").unwrap();
            elapsed
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

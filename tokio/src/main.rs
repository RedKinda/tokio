use tokio::{
    net::unix::pipe::{UringReceiver, UringSender, make_uring_pipe},
    task::JoinHandle,
};

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

async fn test_single_pipe() {
    let (tx, rx) = make_uring_pipe().unwrap();

    let tx_buf = vec![1u8; 100 * 1024];
    let rx_buf = vec![0u8; 100 * 1024];

    let iters = 1000000;

    stress_pipe_split(tx, rx, tx_buf, rx_buf, iters)
        .await
        .unwrap();
}

async fn test_multi_pipe() {
    let tx_buf = vec![1u8; 100 * 1024];
    let rx_buf = vec![0u8; 100 * 1024];

    let iters = 10000;

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
}

fn main() {
    // #[cfg(feature = "tracing")]
    // compile_error!("rahh");

    let mut r = tokio::runtime::Builder::new_multi_thread();

    let r = r.enable_io_uring();

    let rt = r.enable_all().build().unwrap();

    let elapsed = rt.block_on(async move {
        let now = std::time::Instant::now();

        test_single_pipe().await;

        now.elapsed()
    });

    println!("elapsed: {:?}", elapsed);
}

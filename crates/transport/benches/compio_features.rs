//! Isolated receive/scheduling experiment. No production defaults are changed.
//! compio_features <ordinary|managed|multi|poll-first> <default|single|defer|sqpoll> <bytes>
use compio::{
    buf::IntoInner,
    io::{AsyncRead, AsyncReadExt, AsyncReadManaged, AsyncReadMulti, AsyncWriteExt},
};
use futures::StreamExt;
use std::{io, time::Instant};

fn runtime(scheduler: &str, cpu: usize) -> io::Result<compio::runtime::Runtime> {
    let mut driver = compio::driver::ProactorBuilder::new();
    match scheduler {
        "default" => (),
        "single" => {
            driver.single_issuer(true);
        }
        "defer" => {
            driver.single_issuer(true).defer_taskrun(true);
        }
        "sqpoll" => {
            driver.sqpoll_idle(std::time::Duration::from_millis(1000));
        }
        _ => panic!("unknown scheduler"),
    }
    driver
        .buffer_pool_size(32.try_into().unwrap())
        .buffer_pool_buffer_len(65536);
    compio::runtime::RuntimeBuilder::new()
        .with_proactor(driver)
        .thread_affinity([cpu].into())
        .build()
}

async fn receive(stream: &mut compio::net::TcpStream, mode: &str, total: usize) -> io::Result<()> {
    let mut received = 0;
    if mode == "multi" {
        // Linux multishot recv requires len=0; the provided-buffer ring sets
        // each receive's capacity. A nonzero length returns EINVAL on 6.1.
        let chunks = stream.read_multi(0);
        futures::pin_mut!(chunks);
        while received < total {
            let buf = chunks.next().await.ok_or(io::ErrorKind::UnexpectedEof)??;
            received += buf.len();
        }
    } else {
        let mut buf = Vec::with_capacity(65536);
        while received < total {
            if mode == "managed" {
                let chunk = stream
                    .read_managed((total - received).min(65536))
                    .await?
                    .ok_or(io::ErrorKind::UnexpectedEof)?;
                received += chunk.len();
            } else if mode == "poll-first" {
                use compio::driver::{op::Recv, PollFirst};
                let mut op = Recv::new(stream.clone(), buf, rustix::net::RecvFlags::empty());
                op.poll_first();
                let compio::BufResult(result, op) = compio::runtime::submit(op).await;
                buf = op.into_inner();
                let n = result?;
                if n == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                received += n;
            } else {
                let compio::BufResult(result, returned) = stream.read(buf).await;
                buf = returned;
                let n = result?;
                if n == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                received += n;
            }
        }
    }
    assert_eq!(received, total);
    Ok(())
}

fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    assert_eq!(args.len(), 3);
    let mode = args[0].clone();
    let scheduler = args[1].clone();
    let size: usize = args[2].parse().unwrap();
    assert!(matches!(
        mode.as_str(),
        "ordinary" | "managed" | "multi" | "poll-first"
    ));
    let count = (512 * 1024 * 1024 / size).max(1);
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = std::thread::spawn(move || -> io::Result<()> {
        runtime(&scheduler, 42)?.block_on(async {
            let listener = compio::net::TcpListener::from_std(listener)?;
            let (mut stream, _) = listener.accept().await?;
            for _ in 0..2 {
                receive(&mut stream, &mode, count * size).await?;
                stream.write_all(vec![1]).await.0?;
            }
            Ok(())
        })
    });
    let client_result = runtime("default", 40)?.block_on(async {
        let mut conn = compio::net::TcpStream::connect(addr).await?;
        let payload = bytes::Bytes::from(vec![0x6a; size]);
        for stage in 0..2 {
            let mut before: libc::rusage = unsafe { std::mem::zeroed() };
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut before); }
            let start = Instant::now();
            for _ in 0..count { conn.write_all(payload.clone()).await.0?; }
            conn.read_exact(vec![0u8; 1]).await.0?;
            let elapsed = start.elapsed().as_secs_f64();
            let mut after: libc::rusage = unsafe { std::mem::zeroed() };
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut after); }
            let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
            if stage == 1 {
                println!("mode={} scheduler={} size={} bytes={} seconds={elapsed:.6} user={:.6} system={:.6}",
                    args[0], args[1], size, count * size,
                    secs(after.ru_utime)-secs(before.ru_utime), secs(after.ru_stime)-secs(before.ru_stime));
            }
        }
        Ok::<_, io::Error>(())
    });
    let server_result = server.join().expect("server panicked");
    if let Err(error) = &server_result {
        eprintln!("receive experiment failed: {error}");
    }
    client_result.and(server_result)
}

//! A/B benchmark for the Direct-I/O read kernel: bounce buffer versus reading
//! straight into the output's spare capacity.
//!
//! Usage: `direct_read_copy_ab <request-bytes> <rounds> <file> [more files...]`
//!
//! Both challengers duplicate only the small kernel under test, as in
//! `unsafe_hotpath_ab`. Files are opened read-only with `O_DIRECT`; each round
//! reads every file once in aligned requests, alternating kernels, and checks
//! that both kernels return identical bytes. Reported CPU time is the
//! thread's on-CPU time (user plus system), which isolates the copy and zeroing work
//! from storage latency.

#![allow(clippy::cast_precision_loss)]

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::time::{Duration, Instant};

const MEMORY_ALIGNMENT: usize = 4096;

fn bounce_read(file: &File, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    output.try_reserve_exact(length)?;
    let mut bounce = vec![0_u8; length + MEMORY_ALIGNMENT - 1];
    let start = (MEMORY_ALIGNMENT - bounce.as_ptr().addr() % MEMORY_ALIGNMENT) % MEMORY_ALIGNMENT;
    let aligned = &mut bounce[start..start + length];
    let mut done = 0;
    while done < length {
        let count = file.read_at(&mut aligned[done..], offset + done as u64)?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        done += count;
    }
    output.extend_from_slice(aligned);
    Ok(output)
}

fn spare_capacity_read(file: &File, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    output.try_reserve_exact(length)?;
    while output.len() < length {
        let position = offset + output.len() as u64;
        let count = rustix::io::pread(file, rustix::buffer::spare_capacity(&mut output), position)?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
    Ok(output)
}

fn cpu_time() -> Duration {
    // The benchmark is single-threaded; schedstat reports this thread's
    // on-CPU time in nanoseconds, including system time.
    let schedstat = std::fs::read_to_string("/proc/thread-self/schedstat").expect("schedstat");
    let nanos: u64 = schedstat
        .split_whitespace()
        .next()
        .and_then(|field| field.parse().ok())
        .expect("schedstat run time");
    Duration::from_nanos(nanos)
}

fn pass(
    files: &[(File, u64)],
    request: usize,
    kernel: fn(&File, u64, usize) -> io::Result<Vec<u8>>,
) -> (u64, Duration, Duration, blake3::Hash) {
    let mut digest = blake3::Hasher::new();
    let mut bytes = 0_u64;
    let wall = Instant::now();
    let cpu = cpu_time();
    for (file, length) in files {
        let mut offset = 0;
        while offset + request as u64 <= *length {
            let output = kernel(file, offset, request).expect("aligned Direct-I/O read");
            digest.update(&output[..64]);
            bytes += output.len() as u64;
            offset += request as u64;
        }
    }
    (
        bytes,
        wall.elapsed(),
        cpu_time().saturating_sub(cpu),
        digest.finalize(),
    )
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    assert!(
        arguments.len() >= 3,
        "usage: <request-bytes> <rounds> <file>..."
    );
    let request: usize = arguments[0].parse().expect("request bytes");
    let rounds: usize = arguments[1].parse().expect("round count");
    assert!(request.is_multiple_of(MEMORY_ALIGNMENT));
    let files: Vec<(File, u64)> = arguments[2..]
        .iter()
        .map(|path| {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc_o_direct())
                .open(path)
                .expect("open O_DIRECT");
            let length = file.metadata().expect("metadata").len();
            (file, length)
        })
        .collect();
    let mut results = [(Vec::new(), Vec::new()), (Vec::new(), Vec::new())];
    for round in 0..rounds {
        let order = if round % 2 == 0 { [0, 1] } else { [1, 0] };
        let mut digests = [None, None];
        for kernel in order {
            let function = if kernel == 0 {
                bounce_read
            } else {
                spare_capacity_read
            };
            let (bytes, wall, cpu, digest) = pass(&files, request, function);
            let mib = bytes as f64 / 1_048_576.0;
            results[kernel].0.push(mib / wall.as_secs_f64());
            results[kernel].1.push(cpu.as_secs_f64() * 1e6 / mib);
            digests[kernel] = Some(digest);
        }
        assert_eq!(
            digests[0], digests[1],
            "both kernels must read identical bytes"
        );
    }
    for (label, (throughput, cpu)) in ["bounce", "spare-capacity"].iter().zip(results.iter_mut()) {
        println!(
            "kernel={label:<15} request={request} rounds={rounds} median_MiB_s={:.1} median_cpu_us_per_MiB={:.1}",
            median(throughput),
            median(cpu)
        );
    }
}

const fn libc_o_direct() -> i32 {
    // O_DIRECT on x86-64 Linux, the only supported target (ADR 0078).
    0o40000
}

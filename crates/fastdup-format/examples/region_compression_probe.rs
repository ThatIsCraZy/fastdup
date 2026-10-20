//! Region-size vs level compression probe over logical file samples.
//!
//! Usage: region_compression_probe <file> [more files...]
//!
//! For each file, four 64-MiB windows are sampled at spread offsets (the
//! probed stream is the logical content an ingest region would see). Each
//! sample is compressed as independent 512-KiB frames and as 32-MiB frames
//! (full-region window) at levels 3/10/19, plus 32-MiB frames with the
//! level-default window. Reports ratio, compression and decompression
//! throughput, and the cost of extracting the final 512 KiB of a frame
//! (sequential decode-and-discard: the large-frame read tax).
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::time::Instant;

const WINDOW: usize = 64 * 1024 * 1024;
const FRAME_512K: usize = 512 * 1024;
const FRAME_32M: usize = 32 * 1024 * 1024;

struct Config {
    label: &'static str,
    frame: usize,
    level: i32,
    window_log: Option<u32>,
}

fn compress(input: &[u8], level: i32, window_log: Option<u32>) -> Vec<u8> {
    let bound = zstd::zstd_safe::compress_bound(input.len());
    let mut out = vec![0_u8; bound];
    let mut ctx = zstd::zstd_safe::CCtx::try_create().expect("cctx");
    ctx.set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(level))
        .expect("level");
    ctx.set_parameter(zstd::zstd_safe::CParameter::NbWorkers(0))
        .expect("workers");
    ctx.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(false))
        .expect("checksum");
    if let Some(log) = window_log {
        ctx.set_parameter(zstd::zstd_safe::CParameter::WindowLog(log))
            .expect("window log");
    }
    ctx.set_pledged_src_size(Some(input.len() as u64))
        .expect("pledged size");
    let written = ctx.compress2(&mut out, input).expect("compress");
    out.truncate(written);
    out
}

fn decompress(frame: &[u8], output: &mut [u8]) -> usize {
    let mut ctx = zstd::zstd_safe::DCtx::try_create().expect("dctx");
    ctx.decompress(output, frame).expect("decompress")
}

fn sample_windows(path: &PathBuf) -> Vec<u8> {
    let file = File::open(path).expect("open sample file");
    let len = file.metadata().expect("metadata").len();
    let mut data = Vec::new();
    for fraction in [1_u64, 3, 5, 7] {
        let start = len
            .saturating_mul(fraction)
            .saturating_div(8)
            .min(len.saturating_sub(WINDOW as u64));
        let mut buf = vec![0_u8; WINDOW.min(len as usize)];
        if file.read_exact_at(&mut buf, start).is_ok() {
            data.extend_from_slice(&buf);
        }
    }
    data
}

fn main() {
    let paths: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    assert!(
        !paths.is_empty(),
        "usage: region_compression_probe <file>..."
    );
    let configs = [
        Config {
            label: "512K L3 (current)",
            frame: FRAME_512K,
            level: 3,
            window_log: Some(19),
        },
        Config {
            label: "32M L3 full-window",
            frame: FRAME_32M,
            level: 3,
            window_log: Some(25),
        },
        Config {
            label: "32M L3 default-window",
            frame: FRAME_32M,
            level: 3,
            window_log: None,
        },
        Config {
            label: "512K L10",
            frame: FRAME_512K,
            level: 10,
            window_log: Some(19),
        },
        Config {
            label: "32M L10 full-window",
            frame: FRAME_32M,
            level: 10,
            window_log: Some(25),
        },
        Config {
            label: "512K L19",
            frame: FRAME_512K,
            level: 19,
            window_log: Some(19),
        },
        Config {
            label: "32M L19 full-window",
            frame: FRAME_32M,
            level: 19,
            window_log: Some(25),
        },
    ];
    for path in &paths {
        let data = sample_windows(path);
        println!(
            "file={} sampled_MiB={:.0}",
            path.display(),
            data.len() as f64 / 1048576.0
        );
        for config in &configs {
            let started = Instant::now();
            let mut frames: Vec<Vec<u8>> = Vec::new();
            let mut out_bytes = 0_u64;
            for piece in data.chunks(config.frame) {
                let frame = compress(piece, config.level, config.window_log);
                out_bytes += frame.len() as u64;
                frames.push(frame);
            }
            let comp_s = started.elapsed().as_secs_f64();
            // Full decompression of every frame.
            let started = Instant::now();
            let mut scratch = vec![0_u8; config.frame];
            let mut restored = 0_u64;
            for frame in &frames {
                restored += decompress(frame, &mut scratch) as u64;
            }
            let decomp_s = started.elapsed().as_secs_f64();
            assert_eq!(restored, data.len() as u64);
            // Extraction of the final 512 KiB of the last full frame:
            // sequential decode of that frame until the tail materializes.
            let tail_frame = frames
                .iter()
                .find(|frame| frame.len() > 1_000_000)
                .or_else(|| frames.last())
                .expect("at least one frame");
            let started = Instant::now();
            let n = decompress(tail_frame, &mut scratch);
            let extract_ms = started.elapsed().as_secs_f64() * 1000.0;
            println!(
                "  {:24} out_MiB={:9.2} ratio={:5.1}% comp_MiB_s={:8.0} decomp_MiB_s={:8.0} tail_extract_ms={:7.2} (frame_decoded_MiB={:.1})",
                config.label,
                out_bytes as f64 / 1048576.0,
                100.0 * out_bytes as f64 / data.len() as f64,
                data.len() as f64 / 1048576.0 / comp_s,
                data.len() as f64 / 1048576.0 / decomp_s,
                extract_ms,
                n as f64 / 1048576.0,
            );
        }
    }
}

//! Compare the scalar and AVX2/BMI2 `SeqCDC` scanners with FastCDC-v1.
//!
//! This measurement tool imports the appliance's current `SeqCDC` scanner.

use std::env;
use std::fs;
use std::hint::black_box;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use fastcdc::v2020::{FastCDC, Normalization};

use fastdup_store::{SeqCdcConfig, seqcdc_cut, seqcdc_cut_scalar};

const KIB: usize = 1_024;
const MINIMUM_BYTES: usize = 16 * KIB;
const TARGET_BYTES: usize = 64 * KIB;
const MAXIMUM_BYTES: usize = 256 * KIB;
const FASTCDC_SEED: u64 = 0;

#[derive(Clone, Copy)]
struct ScanResult {
    chunks: usize,
    checksum: u64,
    elapsed: Duration,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = env::args().skip(1);
    let path = arguments.next().ok_or_else(|| {
        "usage: seqcdc_challenger FILE [SEQUENCE_LENGTH SKIP_TRIGGER SKIP_SIZE [ROUNDS]]".to_owned()
    })?;
    let sequence_length = parse_argument(arguments.next(), 6, "SEQUENCE_LENGTH")?;
    let skip_trigger = parse_argument(arguments.next(), 50, "SKIP_TRIGGER")?;
    let skip_size = parse_argument(arguments.next(), 1_024, "SKIP_SIZE")?;
    let rounds = parse_argument(arguments.next(), 3, "ROUNDS")?;
    if arguments.next().is_some() {
        return Err("too many arguments".to_owned());
    }

    let bytes = fs::read(&path).map_err(|error| format!("read {path}: {error}"))?;
    let config = SeqCdcConfig {
        sequence_length,
        skip_trigger,
        skip_bytes: skip_size,
        minimum_bytes: MINIMUM_BYTES,
        maximum_bytes: MAXIMUM_BYTES,
    };

    let fastcdc = best_of(rounds, || scan_fastcdc(black_box(&bytes)))?;
    let scalar = best_of(rounds, || {
        scan_seqcdc(black_box(&bytes), config, seqcdc_cut_scalar)
    })?;
    let vector = best_of(rounds, || {
        scan_seqcdc(black_box(&bytes), config, seqcdc_cut)
    })?;
    print_result("fastcdc-v1", bytes.len(), fastcdc);
    print_result("seqcdc-scalar", bytes.len(), scalar);
    print_result("seqcdc-dispatch", bytes.len(), vector);
    println!(
        "seqcdc_config sequence_length={} skip_trigger={} skip_size={} minimum={} maximum={} versus_fastcdc={:.3} versus_scalar={:.3}",
        sequence_length,
        skip_trigger,
        skip_size,
        MINIMUM_BYTES,
        MAXIMUM_BYTES,
        fastcdc.elapsed.as_secs_f64() / vector.elapsed.as_secs_f64(),
        scalar.elapsed.as_secs_f64() / vector.elapsed.as_secs_f64(),
    );
    Ok(())
}

fn parse_argument<T>(argument: Option<String>, default: T, name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    argument.map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| format!("invalid {name} {value:?}: {error}"))
    })
}

fn best_of(rounds: usize, mut scan: impl FnMut() -> (usize, u64)) -> Result<ScanResult, String> {
    if rounds == 0 {
        return Err("ROUNDS must be nonzero".to_owned());
    }
    let mut best = None;
    for _ in 0..rounds {
        let started = Instant::now();
        let (chunks, checksum) = scan();
        let result = ScanResult {
            chunks,
            checksum,
            elapsed: started.elapsed(),
        };
        if best.is_none_or(|previous: ScanResult| result.elapsed < previous.elapsed) {
            best = Some(result);
        }
    }
    Ok(best.expect("ASSERT: a nonzero round count produces one result"))
}

fn scan_fastcdc(bytes: &[u8]) -> (usize, u64) {
    let mut chunks = 0_usize;
    let mut checksum = 0_u64;
    for chunk in FastCDC::with_level_and_seed(
        bytes,
        MINIMUM_BYTES,
        TARGET_BYTES,
        MAXIMUM_BYTES,
        Normalization::Level1,
        FASTCDC_SEED,
    ) {
        chunks += 1;
        checksum = checksum.rotate_left(7) ^ (chunk.offset + chunk.length) as u64;
    }
    black_box((chunks, checksum))
}

fn scan_seqcdc(
    bytes: &[u8],
    config: SeqCdcConfig,
    cut: fn(&[u8], SeqCdcConfig) -> usize,
) -> (usize, u64) {
    let mut offset = 0_usize;
    let mut chunks = 0_usize;
    let mut checksum = 0_u64;
    while offset < bytes.len() {
        let length = cut(&bytes[offset..], config);
        assert!(length != 0 && length <= config.maximum_bytes);
        offset += length;
        chunks += 1;
        checksum = checksum.rotate_left(7) ^ offset as u64;
    }
    black_box((chunks, checksum))
}

#[allow(clippy::cast_precision_loss)]
fn print_result(name: &str, bytes: usize, result: ScanResult) {
    let mib = bytes as f64 / (1_024.0 * 1_024.0);
    println!(
        "algorithm={} bytes={} chunks={} average_chunk_bytes={:.1} elapsed_ms={:.3} throughput_mib_s={:.1} checksum={:016x}",
        name,
        bytes,
        result.chunks,
        bytes as f64 / result.chunks as f64,
        result.elapsed.as_secs_f64() * 1_000.0,
        mib / result.elapsed.as_secs_f64(),
        result.checksum,
    );
}

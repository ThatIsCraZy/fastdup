use std::env;
use std::fs::File;
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::process::ExitCode;
use std::time::Instant;

use fastdup_format::{
    ContainerId, IncompressibilityGateMetrics, IncompressibilityGatePolicy, SealedContainer,
};

const CONTAINER_INPUT_BYTES: usize = 32 * 1_024 * 1_024;
const CHUNK_BYTES: usize = 64 * 1_024;
const CHUNKS_PER_REGION: usize = 8;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("incompressibility_gate_matrix: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let path = arguments.next().ok_or(
        "usage: incompressibility_gate_matrix PATH WORKERS POLICY[v1|lz4-only|off] [MAX_BYTES]",
    )?;
    let workers = arguments
        .next()
        .ok_or("missing WORKERS")?
        .parse::<NonZeroUsize>()?;
    let (policy_name, policy) = match arguments.next().as_deref() {
        Some("v1") => ("v1", IncompressibilityGatePolicy::V1),
        Some("lz4-only") => ("lz4-only", IncompressibilityGatePolicy::Lz4Only),
        Some("off") => ("off", IncompressibilityGatePolicy::Off),
        _ => return Err("POLICY must be v1, lz4-only, or off".into()),
    };
    let maximum_bytes = arguments
        .next()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(u64::MAX);
    if arguments.next().is_some() {
        return Err("too many arguments".into());
    }

    let mut input = File::open(path)?;
    let mut buffer = vec![0_u8; CONTAINER_INPUT_BYTES];
    let mut logical_bytes = 0_u64;
    let mut container_bytes = 0_u64;
    let mut container_count = 0_u64;
    let mut metrics = IncompressibilityGateMetrics::default();
    let started = Instant::now();

    while logical_bytes < maximum_bytes {
        let remaining = maximum_bytes.saturating_sub(logical_bytes);
        let limit = usize::try_from(remaining.min(CONTAINER_INPUT_BYTES as u64))?;
        let used = read_up_to(&mut input, &mut buffer[..limit])?;
        if used == 0 {
            break;
        }
        let data = &buffer[..used];
        let chunks = data.chunks(CHUNK_BYTES).collect::<Vec<_>>();
        let regions = chunks.chunks(CHUNKS_PER_REGION).collect::<Vec<_>>();
        let ordinal = container_count
            .checked_add(1)
            .ok_or("container ordinal overflow")?;
        let encoded = SealedContainer::encode_adaptive_regions_parallel_profiled_with_gate(
            container_id(ordinal)?,
            ordinal,
            &regions,
            workers,
            policy,
        )?;
        let decoded = SealedContainer::decode(encoded.bytes())?;
        let mut offset = 0_usize;
        for record in decoded.records() {
            let end = offset
                .checked_add(record.payload().len())
                .ok_or("decoded offset overflow")?;
            if data.get(offset..end) != Some(record.payload()) {
                return Err("decoded Container differs from the input stream".into());
            }
            offset = end;
        }
        if offset != data.len() {
            return Err("decoded Container ended before the input stream".into());
        }
        metrics.checked_merge(encoded.metrics())?;
        logical_bytes = logical_bytes
            .checked_add(u64::try_from(used)?)
            .ok_or("logical byte counter overflow")?;
        container_bytes = container_bytes
            .checked_add(u64::try_from(encoded.bytes().len())?)
            .ok_or("Container byte counter overflow")?;
        container_count = ordinal;
    }

    let elapsed = started.elapsed();
    let elapsed_ns = u64::try_from(elapsed.as_nanos())?;
    let bytes_per_second = if elapsed_ns == 0 {
        0
    } else {
        u64::try_from(u128::from(logical_bytes) * 1_000_000_000 / u128::from(elapsed_ns))?
    };
    println!(concat!(
        "policy,workers,logical_bytes,container_bytes,containers,elapsed_ns,bytes_per_second,",
        "disabled,eligible,size_bypass,lz4_allowed,lz4_rejected,zstd1_allowed,zstd1_rejected,",
        "target_trials,target_accepted,target_rejected,raw_after_gate,scratch_hwm"
    ));
    println!(
        "{policy_name},{workers},{logical_bytes},{container_bytes},{container_count},{elapsed_ns},{bytes_per_second},{},{},{},{},{},{},{},{},{},{},{},{}",
        metrics.disabled_regions(),
        metrics.eligible_regions(),
        metrics.size_bypassed_regions(),
        metrics.lz4_allowed_regions(),
        metrics.lz4_rejected_regions(),
        metrics.zstd1_allowed_regions(),
        metrics.zstd1_rejected_regions(),
        metrics.target_zstd_trials(),
        metrics.target_zstd_accepted(),
        metrics.target_zstd_rejected(),
        metrics.raw_regions_after_gate(),
        metrics.scratch_high_water_bytes(),
    );
    Ok(())
}

fn read_up_to(input: &mut File, mut destination: &mut [u8]) -> io::Result<usize> {
    let mut total = 0_usize;
    while !destination.is_empty() {
        let read = input.read(destination)?;
        if read == 0 {
            break;
        }
        total += read;
        destination = &mut destination[read..];
    }
    Ok(total)
}

fn container_id(ordinal: u64) -> Result<ContainerId, Box<dyn std::error::Error>> {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&ordinal.to_le_bytes());
    bytes[8..].copy_from_slice(b"gate-v1!");
    ContainerId::new(bytes)
        .map_err(|_| "nonzero ordinal unexpectedly produced a zero Container ID".into())
}

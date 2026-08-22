use std::error::Error;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use fastdup_appliance::{
    DurableNamespace, ProofCachePolicy, ProofCacheTrace, checkpoint_policy_set_v1,
    replay_proof_cache_trace,
};
use fastdup_posix::{NamespaceConfig, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, GenerationRepository,
};

const CALLER: RequestContext = RequestContext {
    uid: 0,
    gid: 0,
    pid: 1,
};
const WRITE_BYTES: usize = 1024 * 1024;
const MAX_TRACE_EVENTS: usize = 4_000_000;
type VariantEdit = (u64, [u8; 32]);

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    match args.first().and_then(|arg| arg.to_str()) {
        Some("record") => record(&args[1..]),
        Some("replay") => replay(&args[1..]),
        _ => Err(usage().into()),
    }
}

fn record(args: &[std::ffi::OsString]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        return Err(usage().into());
    }
    let metadata_root = PathBuf::from(&args[0]);
    let data_root = PathBuf::from(&args[1]);
    let trace_path = PathBuf::from(&args[2]);
    let work = if args[3].to_str() == Some("--variants") {
        if args.len() != 6 {
            return Err(usage().into());
        }
        let count = args[4]
            .to_str()
            .ok_or("variant count is not UTF-8")?
            .parse::<usize>()?;
        if count == 0 || count > 100 {
            return Err("variant count must be between 1 and 100".into());
        }
        let source = PathBuf::from(&args[5]);
        (0..count)
            .map(|ordinal| {
                (
                    source.clone(),
                    Some(u64::try_from(ordinal).expect("bounded count")),
                )
            })
            .collect::<Vec<_>>()
    } else {
        args[3..]
            .iter()
            .map(|source| (PathBuf::from(source), None))
            .collect::<Vec<_>>()
    };
    fs::create_dir_all(&metadata_root)?;
    fs::create_dir_all(&data_root)?;
    if trace_path.exists() {
        return Err(format!("trace output already exists: {}", trace_path.display()).into());
    }
    let index_root = metadata_root.join("exact-index");
    let generation_root = metadata_root.join("generations");
    let container_root = data_root.join("containers");
    let indexes = ExactIndexRunRepository::new(FsStorageIo::open(&index_root)?);
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(
            FsStorageIo::open(&generation_root)?,
            checkpoint_policy_set_v1(),
        ),
        ContainerRepository::new(FsStorageIo::open(&container_root)?),
        &indexes,
        1024,
    )?;
    appliance.start_online_proof_trace(MAX_TRACE_EVENTS)?;
    for (ordinal, (source, variant)) in work.iter().enumerate() {
        ingest_file(&appliance, source, ordinal, *variant)?;
        appliance
            .checkpoint()?
            .ok_or("input did not produce a namespace generation")?;
    }
    let trace = appliance.finish_online_proof_trace()?;
    let encoded = trace.encode()?;
    let mut output = File::options()
        .write(true)
        .create_new(true)
        .open(&trace_path)?;
    output.write_all(&encoded)?;
    output.sync_all()?;
    let parent = trace_path.parent().ok_or("trace path has no parent")?;
    File::open(parent)?.sync_all()?;
    println!(
        "recorded_events={} encoded_bytes={} inputs={} trace={}",
        trace.events().len(),
        encoded.len(),
        work.len(),
        trace_path.display()
    );
    Ok(())
}

fn ingest_file<M, C>(
    appliance: &DurableNamespace<M, C>,
    source: &Path,
    ordinal: usize,
    variant: Option<u64>,
) -> Result<(), Box<dyn Error>>
where
    M: Clone + Send + Sync + fastdup_store::StorageIo + 'static,
    C: Clone + Send + Sync + fastdup_store::StorageIo + 'static,
{
    let name = format!("proof-trace-{ordinal:04}.img");
    let Reply::Created { entry, handle } = posix(appliance.namespace().dispatch(
        CALLER,
        Operation::Create {
            parent: ROOT_INODE,
            name: name.as_bytes(),
            mode: 0o600,
            options: OpenOptions::READ_WRITE,
            exclusive: true,
            truncate: false,
        },
    ))?
    else {
        return Err("create returned an unexpected reply".into());
    };
    let mut input = File::open(source)?;
    let edits = variant_edits(input.metadata()?.len(), variant)?;
    let mut buffer = vec![0_u8; WRITE_BYTES];
    let mut offset = 0_u64;
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        apply_variant_edits(&mut buffer[..read], offset, &edits)?;
        posix(appliance.namespace().dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset,
                data: &buffer[..read],
            },
        ))?;
        offset = offset
            .checked_add(u64::try_from(read)?)
            .ok_or("input offset overflow")?;
    }
    posix(appliance.namespace().dispatch(
        CALLER,
        Operation::Release {
            inode: entry.attr.inode,
            handle,
        },
    ))?;
    Ok(())
}

fn replay(args: &[std::ffi::OsString]) -> Result<(), Box<dyn Error>> {
    if args.len() < 2 {
        return Err(usage().into());
    }
    let trace_path = PathBuf::from(&args[0]);
    let trace = ProofCacheTrace::decode(&fs::read(&trace_path)?)?;
    println!(
        "policy,budget_bytes,capacity,events,lookups,hits,misses,hit_rate_basis_points,admissions,admission_rejections,evictions,avoided_verify_bytes,required_verify_bytes,max_eviction_steps"
    );
    for budget in &args[1..] {
        let budget = budget
            .to_str()
            .ok_or("budget is not UTF-8")?
            .parse::<u64>()?;
        for policy in [ProofCachePolicy::S3Fifo, ProofCachePolicy::Sieve] {
            let report = replay_proof_cache_trace(&trace, policy, budget)?;
            let hit_rate_basis_points = if report.lookups() == 0 {
                0
            } else {
                report
                    .hits()
                    .checked_mul(10_000)
                    .ok_or("hit-rate overflow")?
                    / report.lookups()
            };
            let policy_name = match policy {
                ProofCachePolicy::S3Fifo => "s3-fifo",
                ProofCachePolicy::Sieve => "sieve",
            };
            println!(
                "{policy_name},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                report.byte_budget(),
                report.capacity(),
                trace.events().len(),
                report.lookups(),
                report.hits(),
                report.misses(),
                hit_rate_basis_points,
                report.admissions(),
                report.admission_rejections(),
                report.evictions(),
                report.avoided_verify_bytes(),
                report.required_verify_bytes(),
                report.maximum_eviction_steps(),
            );
        }
    }
    Ok(())
}

fn usage() -> &'static str {
    "usage:\n  proof_cache_trace record <metadata-root> <data-root> <trace-output> <input>...\n  proof_cache_trace record <metadata-root> <data-root> <trace-output> --variants <count> <input>\n  proof_cache_trace replay <trace-input> <budget-bytes>..."
}

fn posix<T>(result: Result<T, fastdup_posix::PosixError>) -> Result<T, std::io::Error> {
    result.map_err(|error| std::io::Error::other(format!("POSIX operation failed: {error:?}")))
}

fn variant_edits(
    file_length: u64,
    variant: Option<u64>,
) -> Result<Vec<VariantEdit>, Box<dyn Error>> {
    let Some(variant) = variant else {
        return Ok(Vec::new());
    };
    if variant == 0 {
        return Ok(Vec::new());
    }
    let available = file_length
        .checked_sub(32)
        .ok_or("variant source is shorter than 32 bytes")?;
    let mut edits = Vec::with_capacity(8);
    for edit in 0_u64..8 {
        let seed = splitmix64(variant ^ edit.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let offset = seed % available;
        let mut replacement = [0_u8; 32];
        for (index, byte) in replacement.iter_mut().enumerate() {
            *byte = splitmix64(
                seed ^ u64::try_from(index).expect("32-byte replacement index fits u64"),
            )
            .to_le_bytes()[0];
        }
        edits.push((offset, replacement));
    }
    edits.sort_unstable_by_key(|(offset, _)| *offset);
    Ok(edits)
}

fn apply_variant_edits(
    buffer: &mut [u8],
    buffer_offset: u64,
    edits: &[VariantEdit],
) -> Result<(), Box<dyn Error>> {
    let buffer_end = buffer_offset
        .checked_add(u64::try_from(buffer.len())?)
        .ok_or("variant buffer range overflow")?;
    for (edit_offset, replacement) in edits {
        let edit_end = edit_offset
            .checked_add(32)
            .ok_or("variant edit range overflow")?;
        let start = (*edit_offset).max(buffer_offset);
        let end = edit_end.min(buffer_end);
        if start >= end {
            continue;
        }
        let buffer_start = usize::try_from(start - buffer_offset)?;
        let replacement_start = usize::try_from(start - edit_offset)?;
        let length = usize::try_from(end - start)?;
        buffer[buffer_start..buffer_start + length]
            .copy_from_slice(&replacement[replacement_start..replacement_start + length]);
    }
    Ok(())
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

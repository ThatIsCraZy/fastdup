use std::env;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use fastdup_store::{
    ReducedObject, ReductionDictionary, ReductionEngine, ReductionFeatures, ReductionPolicy,
    ReductionReport, ReductionRuntime,
};

const DEFAULT_INFLIGHT_MIB: usize = 64;
const DEFAULT_DICTIONARY_KIB: usize = 64;
const DICTIONARY_TRAINING_SAMPLE_BYTES: usize = 16 * 1_024;
const KIB: usize = 1_024;
const MIB: usize = 1_024 * 1_024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse(env::args_os().skip(1))?;
    let maximum_inflight_bytes = options
        .inflight_mib
        .checked_mul(MIB)
        .ok_or("--inflight-mib byte count overflows usize")?;
    let policy = ReductionPolicy::v1(options.features)?;
    let runtime = ReductionRuntime::new(options.workers, maximum_inflight_bytes)?;
    let dictionary = options.train_dictionary()?;
    let mut engine = dictionary.as_ref().map_or_else(
        || Ok(ReductionEngine::new(policy, runtime)),
        |dictionary| ReductionEngine::with_dictionary(policy, runtime, dictionary),
    )?;
    let mut objects = Vec::with_capacity(options.inputs.len());
    let mut totals = Totals::default();

    let run_started = Instant::now();
    let ingest_started = Instant::now();
    for path in &options.inputs {
        let input = std::fs::read(path)?;
        let object = engine.ingest(&input)?;
        totals.add(engine.report(object)?)?;
        objects.push((path.clone(), object));
    }
    let ingest_elapsed = ingest_started.elapsed();

    let restore_started = Instant::now();
    verify_restores(&engine, &objects)?;
    let restore_elapsed = restore_started.elapsed();
    let elapsed = run_started.elapsed();

    print_csv(
        &options,
        policy,
        totals,
        ingest_elapsed,
        restore_elapsed,
        elapsed,
        dictionary.as_ref(),
    );
    Ok(())
}

#[derive(Debug)]
struct Options {
    policy_name: String,
    features: ReductionFeatures,
    workers: NonZeroUsize,
    inflight_mib: usize,
    dictionary_kib: Option<usize>,
    dictionary_samples: Vec<PathBuf>,
    inputs: Vec<PathBuf>,
}

impl Options {
    fn parse(
        arguments: impl Iterator<Item = OsString>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut arguments = arguments.peekable();
        let mut workers = std::thread::available_parallelism()
            .unwrap_or_else(|_| NonZeroUsize::new(1).expect("ASSERT: one is nonzero"));
        let mut inflight_mib = DEFAULT_INFLIGHT_MIB;
        let mut dictionary_kib = None;
        let mut dictionary_samples = Vec::new();
        let mut preset = None;
        let mut explicit_features = None;
        let mut inputs = Vec::new();
        let mut positional_only = false;

        while let Some(argument) = arguments.next() {
            if positional_only {
                inputs.push(PathBuf::from(argument));
                continue;
            }
            let Some(text) = argument.to_str() else {
                inputs.push(PathBuf::from(argument));
                continue;
            };
            match text {
                "--" => positional_only = true,
                "--workers" => {
                    workers = parse_nonzero("--workers", arguments.next())?;
                }
                "--inflight-mib" => {
                    inflight_mib = parse_nonzero("--inflight-mib", arguments.next())?.get();
                }
                "--dictionary-kib" => {
                    dictionary_kib =
                        Some(parse_nonzero("--dictionary-kib", arguments.next())?.get());
                }
                "--dictionary-sample" => {
                    let sample = arguments
                        .next()
                        .ok_or("--dictionary-sample requires a path")?;
                    dictionary_samples.push(PathBuf::from(sample));
                }
                "--preset" => {
                    if explicit_features.is_some() || preset.is_some() {
                        return Err("choose exactly one --preset or a set of feature flags".into());
                    }
                    let value = utf8_value("--preset", arguments.next())?;
                    if preset_features(&value).is_none() {
                        return Err(format!("unknown reduction preset {value:?}").into());
                    }
                    preset = Some(value);
                }
                _ => {
                    if let Some(feature) = feature_flag(text) {
                        if preset.is_some() {
                            return Err("feature flags cannot be combined with --preset".into());
                        }
                        explicit_features =
                            Some(explicit_features.map_or(feature, |selected| selected | feature));
                    } else if text.starts_with('-') {
                        return Err(format!("unknown option {text:?}; {USAGE}").into());
                    } else {
                        inputs.push(PathBuf::from(argument));
                    }
                }
            }
        }

        if inputs.is_empty() {
            return Err(USAGE.into());
        }
        let (policy_name, features) = if let Some(name) = preset {
            let features = preset_features(&name)
                .expect("ASSERT: a validated preset must still have a feature set");
            (name, features)
        } else if let Some(features) = explicit_features {
            (feature_name(features), features)
        } else {
            ("raw".to_owned(), ReductionFeatures::RAW)
        };
        if dictionary_samples.is_empty() && dictionary_kib.is_some() {
            return Err("--dictionary-kib requires at least one --dictionary-sample".into());
        }
        if !dictionary_samples.is_empty() && !features.contains(ReductionFeatures::COMPRESSION) {
            return Err("dictionary training requires COMPRESSION".into());
        }
        Ok(Self {
            policy_name,
            features,
            workers,
            inflight_mib,
            dictionary_kib,
            dictionary_samples,
            inputs,
        })
    }

    fn train_dictionary(&self) -> Result<Option<ReductionDictionary>, Box<dyn std::error::Error>> {
        if self.dictionary_samples.is_empty() {
            return Ok(None);
        }
        let maximum_bytes = self
            .dictionary_kib
            .unwrap_or(DEFAULT_DICTIONARY_KIB)
            .checked_mul(KIB)
            .ok_or("--dictionary-kib byte count overflows usize")?;
        let training_files = self
            .dictionary_samples
            .iter()
            .map(std::fs::read)
            .collect::<Result<Vec<_>, _>>()?;
        if training_files.iter().any(Vec::is_empty) {
            return Err("dictionary training files must be nonempty".into());
        }
        let samples = training_files
            .iter()
            .flat_map(|file| file.chunks(DICTIONARY_TRAINING_SAMPLE_BYTES))
            .collect::<Vec<_>>();
        Ok(Some(ReductionDictionary::train_v1(
            &samples,
            maximum_bytes,
        )?))
    }
}

const USAGE: &str = "usage: reduction_matrix [--workers N] [--inflight-mib N] \
    [--dictionary-sample FILE ...] [--dictionary-kib N] \
    [--preset raw|cdc|exact|compression|grouping|similarity|delta|reorder|all | \
    --raw --cdc --exact --compression --grouping --similarity --delta --reorder --all] \
    FILE [FILE ...]";

fn parse_nonzero(
    option: &str,
    value: Option<OsString>,
) -> Result<NonZeroUsize, Box<dyn std::error::Error>> {
    let value = utf8_value(option, value)?;
    let parsed = value.parse::<usize>()?;
    NonZeroUsize::new(parsed).ok_or_else(|| format!("{option} must be greater than zero").into())
}

fn utf8_value(option: &str, value: Option<OsString>) -> Result<String, Box<dyn std::error::Error>> {
    value
        .ok_or_else(|| format!("{option} requires a value"))?
        .into_string()
        .map_err(|_| format!("{option} requires a UTF-8 value").into())
}

fn feature_flag(argument: &str) -> Option<ReductionFeatures> {
    match argument {
        "--raw" => Some(ReductionFeatures::RAW),
        "--cdc" => Some(ReductionFeatures::CDC),
        "--exact" => Some(ReductionFeatures::EXACT),
        "--compression" => Some(ReductionFeatures::COMPRESSION),
        "--grouping" => Some(ReductionFeatures::GROUPING),
        "--similarity" => Some(ReductionFeatures::SIMILARITY),
        "--delta" => Some(ReductionFeatures::DELTA),
        "--reorder" => Some(ReductionFeatures::REORDER),
        "--all" => Some(ReductionFeatures::ALL),
        _ => None,
    }
}

fn preset_features(name: &str) -> Option<ReductionFeatures> {
    let raw = ReductionFeatures::RAW;
    let cdc = raw | ReductionFeatures::CDC;
    match name {
        "raw" => Some(raw),
        "cdc" => Some(cdc),
        "exact" => Some(cdc | ReductionFeatures::EXACT),
        "compression" => Some(cdc | ReductionFeatures::COMPRESSION),
        "grouping" => Some(cdc | ReductionFeatures::COMPRESSION | ReductionFeatures::GROUPING),
        "similarity" => Some(cdc | ReductionFeatures::EXACT | ReductionFeatures::SIMILARITY),
        "delta" => Some(
            cdc | ReductionFeatures::EXACT
                | ReductionFeatures::SIMILARITY
                | ReductionFeatures::DELTA,
        ),
        "reorder" => Some(
            cdc | ReductionFeatures::COMPRESSION
                | ReductionFeatures::GROUPING
                | ReductionFeatures::REORDER,
        ),
        "all" => Some(ReductionFeatures::ALL),
        _ => None,
    }
}

fn feature_name(features: ReductionFeatures) -> String {
    let mut names = Vec::new();
    for (name, feature) in [
        ("raw", ReductionFeatures::RAW),
        ("cdc", ReductionFeatures::CDC),
        ("exact", ReductionFeatures::EXACT),
        ("compression", ReductionFeatures::COMPRESSION),
        ("grouping", ReductionFeatures::GROUPING),
        ("similarity", ReductionFeatures::SIMILARITY),
        ("delta", ReductionFeatures::DELTA),
        ("reorder", ReductionFeatures::REORDER),
    ] {
        if features.contains(feature) {
            names.push(name);
        }
    }
    names.join("+")
}

fn verify_restores(
    engine: &ReductionEngine,
    objects: &[(PathBuf, ReducedObject)],
) -> Result<(), Box<dyn std::error::Error>> {
    for (path, object) in objects {
        let expected = std::fs::read(path)?;
        let restored = engine.restore(*object)?;
        if restored != expected {
            return Err(format!("byte-exact restore mismatch for {}", path.display()).into());
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct Totals {
    logical_bytes: u64,
    physical_payload_bytes: u64,
    exact_hit_bytes: u64,
    logical_chunks: usize,
    raw_chunks: usize,
    zstd_regions: usize,
    zstd_dictionary_regions: usize,
    delta_chunks: usize,
    fill_extents: usize,
    fill_bytes: u64,
    similarity_candidates: usize,
    delta_trials: usize,
    delta_logical_bytes: u64,
    delta_payload_bytes: u64,
    maximum_delta_depth: u8,
    reordered_regions: usize,
    placement_windows: usize,
    exact_hits: usize,
    maximum_workers_used: usize,
}

impl Totals {
    fn add(&mut self, report: ReductionReport) -> Result<(), Box<dyn std::error::Error>> {
        self.logical_bytes = self
            .logical_bytes
            .checked_add(report.logical_bytes())
            .ok_or("aggregate logical bytes overflow u64")?;
        self.physical_payload_bytes = self
            .physical_payload_bytes
            .checked_add(report.physical_payload_bytes())
            .ok_or("aggregate physical bytes overflow u64")?;
        self.exact_hit_bytes = self
            .exact_hit_bytes
            .checked_add(report.exact_hit_bytes())
            .ok_or("aggregate Exact Hit bytes overflow u64")?;
        self.logical_chunks = self
            .logical_chunks
            .checked_add(report.logical_chunks())
            .ok_or("aggregate logical chunks overflow usize")?;
        self.raw_chunks = self
            .raw_chunks
            .checked_add(report.raw_chunks())
            .ok_or("aggregate RAW chunks overflow usize")?;
        self.zstd_regions = self
            .zstd_regions
            .checked_add(report.zstd_regions())
            .ok_or("aggregate Zstd regions overflow usize")?;
        self.zstd_dictionary_regions = self
            .zstd_dictionary_regions
            .checked_add(report.zstd_dictionary_regions())
            .ok_or("aggregate dictionary Zstd regions overflow usize")?;
        self.delta_chunks = self
            .delta_chunks
            .checked_add(report.delta_chunks())
            .ok_or("aggregate Delta chunks overflow usize")?;
        self.fill_extents = self
            .fill_extents
            .checked_add(report.fill_extents())
            .ok_or("aggregate FILL extents overflow usize")?;
        self.fill_bytes = self
            .fill_bytes
            .checked_add(report.fill_bytes())
            .ok_or("aggregate FILL bytes overflow u64")?;
        self.similarity_candidates = self
            .similarity_candidates
            .checked_add(report.similarity_candidates())
            .ok_or("aggregate Similarity candidates overflow usize")?;
        self.delta_trials = self
            .delta_trials
            .checked_add(report.delta_trials())
            .ok_or("aggregate Delta trials overflow usize")?;
        self.delta_logical_bytes = self
            .delta_logical_bytes
            .checked_add(report.delta_logical_bytes())
            .ok_or("aggregate Delta logical bytes overflow u64")?;
        self.delta_payload_bytes = self
            .delta_payload_bytes
            .checked_add(report.delta_payload_bytes())
            .ok_or("aggregate Delta payload bytes overflow u64")?;
        self.maximum_delta_depth = self.maximum_delta_depth.max(report.maximum_delta_depth());
        self.reordered_regions = self
            .reordered_regions
            .checked_add(report.reordered_regions())
            .ok_or("aggregate reordered regions overflow usize")?;
        self.placement_windows = self
            .placement_windows
            .checked_add(report.placement_windows())
            .ok_or("aggregate placement windows overflow usize")?;
        self.exact_hits = self
            .exact_hits
            .checked_add(report.exact_hits())
            .ok_or("aggregate Exact Hits overflow usize")?;
        self.maximum_workers_used = self.maximum_workers_used.max(report.workers_used());
        Ok(())
    }
}

fn print_csv(
    options: &Options,
    policy: ReductionPolicy,
    totals: Totals,
    ingest_elapsed: Duration,
    restore_elapsed: Duration,
    elapsed: Duration,
    dictionary: Option<&ReductionDictionary>,
) {
    let dictionary_id = dictionary.map_or_else(|| "-".to_owned(), |value| encode_hex(value.id()));
    let dictionary_bytes = dictionary.map_or(0, ReductionDictionary::len);
    println!(
        concat!(
            "{},{},{},{},{},{},",
            "{},{},{},{},{},{},",
            "{},{},{},{},{},{},",
            "{},{},{},{},{},{},{:.6},{:.6},{:.6},{},{},{},{}"
        ),
        options.policy_name,
        encode_hex(policy.id()),
        options.inputs.len(),
        totals.logical_bytes,
        totals.physical_payload_bytes,
        totals.exact_hit_bytes,
        totals.logical_chunks,
        totals.exact_hits,
        totals.raw_chunks,
        totals.zstd_regions,
        totals.zstd_dictionary_regions,
        totals.delta_chunks,
        totals.fill_extents,
        totals.fill_bytes,
        totals.similarity_candidates,
        totals.delta_trials,
        totals.delta_logical_bytes,
        totals.delta_payload_bytes,
        totals.maximum_delta_depth,
        totals.reordered_regions,
        totals.placement_windows,
        options.workers,
        totals.maximum_workers_used,
        options.inflight_mib,
        ingest_elapsed.as_secs_f64(),
        restore_elapsed.as_secs_f64(),
        elapsed.as_secs_f64(),
        bytes_per_second(totals.logical_bytes, ingest_elapsed),
        bytes_per_second(totals.logical_bytes, restore_elapsed),
        dictionary_id,
        dictionary_bytes,
    );
}

fn bytes_per_second(bytes: u64, elapsed: Duration) -> u128 {
    let nanoseconds = elapsed.as_nanos();
    if nanoseconds == 0 {
        return 0;
    }
    u128::from(bytes) * 1_000_000_000 / nanoseconds
}

fn encode_hex(bytes: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02x}").expect("ASSERT: writing to a String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{Options, ReductionFeatures};

    fn arguments(values: &[&str]) -> impl Iterator<Item = OsString> {
        values
            .iter()
            .map(|value| OsString::from(*value))
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn dictionary_options_require_compression_and_training_samples() {
        assert!(
            Options::parse(arguments(&[
                "--preset",
                "exact",
                "--dictionary-sample",
                "training.json",
                "target.json",
            ]))
            .is_err()
        );
        assert!(
            Options::parse(arguments(&[
                "--preset",
                "grouping",
                "--dictionary-kib",
                "64",
                "target.json",
            ]))
            .is_err()
        );
    }

    #[test]
    fn dictionary_samples_are_not_positional_ingest_inputs() {
        let options = Options::parse(arguments(&[
            "--preset",
            "grouping",
            "--dictionary-kib",
            "32",
            "--dictionary-sample",
            "training-1.json",
            "--dictionary-sample",
            "training-2.json",
            "target.json",
        ]))
        .expect("dictionary benchmark options are valid");

        assert!(options.features.contains(ReductionFeatures::COMPRESSION));
        assert_eq!(options.dictionary_kib, Some(32));
        assert_eq!(options.dictionary_samples.len(), 2);
        assert_eq!(options.inputs.len(), 1);
    }
}

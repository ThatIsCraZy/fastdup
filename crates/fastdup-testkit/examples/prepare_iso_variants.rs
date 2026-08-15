use std::collections::BTreeMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use fastdup_testkit::{ByteMutation, minimal_variant_plan};

const EXPECTED_ISO_BYTES: u64 = 2_072_444_928;
const EXPECTED_ISO_SHA256: &str =
    "aac6ac3ce781b91a91ce78463405f66c611a5dca4b3840c79e5e01d97302f6c8";
const VARIANT_COUNT: usize = 10;
const EDITS_PER_VARIANT: usize = 8;
const MUTATION_SEED: u64 = 0x4d59_5df4_d0f3_3173;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let base = arguments.next().map_or_else(default_base, PathBuf::from);
    let output = arguments.next().map_or_else(
        || base.with_file_name("rocky-minimal-variants-v1"),
        PathBuf::from,
    );
    if arguments.next().is_some() {
        return Err("usage: prepare_iso_variants [base-iso] [output-directory]".into());
    }

    validate_base(&base)?;
    create_empty_directory(&output)?;
    let plans = minimal_variant_plan(
        EXPECTED_ISO_BYTES,
        VARIANT_COUNT,
        EDITS_PER_VARIANT,
        MUTATION_SEED,
    )?;
    let manifest_path = output.join("manifest-v1.tsv");
    let manifest_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&manifest_path)?;
    let mut manifest = BufWriter::new(manifest_file);
    writeln!(manifest, "format\tfastdup-iso-variants-v1")?;
    writeln!(manifest, "base_bytes\t{EXPECTED_ISO_BYTES}")?;
    writeln!(manifest, "base_sha256\t{EXPECTED_ISO_SHA256}")?;
    writeln!(manifest, "seed\t{MUTATION_SEED:#018x}")?;
    writeln!(manifest, "edits_per_variant\t{EDITS_PER_VARIANT}")?;

    for (ordinal, mutations) in plans.iter().enumerate() {
        let name = format!("Rocky-10.2-x86_64-minimal.variant-{ordinal:02}.iso");
        let target = output.join(&name);
        reflink_copy(&base, &target)?;
        let observed = apply_mutations(&target, mutations)?;
        verify_exact_differences(&base, &target, &observed)?;
        let sha256 = sha256sum(&target)?;
        writeln!(
            manifest,
            "variant\t{ordinal:02}\t{name}\t{sha256}\t{}",
            observed.len()
        )?;
        for mutation in observed.values() {
            writeln!(
                manifest,
                "mutation\t{ordinal:02}\t{}\t{:02x}\t{:02x}\t{:02x}",
                mutation.offset, mutation.old, mutation.new, mutation.xor_mask
            )?;
        }
        println!("variant={name} sha256={sha256} edits={}", observed.len());
    }

    manifest.flush()?;
    manifest.get_ref().sync_all()?;
    File::open(&output)?.sync_all()?;
    validate_base(&base)?;
    println!("manifest={}", manifest_path.display());
    Ok(())
}

fn default_base() -> PathBuf {
    PathBuf::from("/source/fastdup/.artifacts/tier-data/corpus/Rocky-10.2-x86_64-minimal.iso")
}

fn validate_base(base: &Path) -> io::Result<()> {
    let metadata = base.metadata()?;
    if !metadata.is_file() || metadata.len() != EXPECTED_ISO_BYTES {
        return Err(invalid_data("base ISO identity or length mismatch"));
    }
    if sha256sum(base)? != EXPECTED_ISO_SHA256 {
        return Err(invalid_data("base ISO SHA-256 mismatch"));
    }
    Ok(())
}

fn create_empty_directory(path: &Path) -> io::Result<()> {
    std::fs::create_dir(path)
}

fn reflink_copy(source: &Path, target: &Path) -> io::Result<()> {
    let status = Command::new("cp")
        .arg("--reflink=always")
        .arg("--")
        .arg(source)
        .arg(target)
        .status()?;
    if !status.success() {
        return Err(io::Error::other("cp --reflink=always failed"));
    }
    Ok(())
}

fn apply_mutations(
    target: &Path,
    mutations: &[ByteMutation],
) -> io::Result<BTreeMap<u64, ObservedMutation>> {
    let file = OpenOptions::new().read(true).write(true).open(target)?;
    let mut observed = BTreeMap::new();
    for mutation in mutations {
        let mut byte = [0_u8; 1];
        file.read_exact_at(&mut byte, mutation.offset)?;
        let old = byte[0];
        let new = old ^ mutation.xor_mask;
        file.write_all_at(&[new], mutation.offset)?;
        observed.insert(
            mutation.offset,
            ObservedMutation {
                offset: mutation.offset,
                old,
                new,
                xor_mask: mutation.xor_mask,
            },
        );
    }
    file.sync_all()?;
    Ok(observed)
}

fn verify_exact_differences(
    base: &Path,
    variant: &Path,
    expected: &BTreeMap<u64, ObservedMutation>,
) -> io::Result<()> {
    if variant.metadata()?.len() != EXPECTED_ISO_BYTES {
        return Err(invalid_data("variant length changed"));
    }
    let output = Command::new("cmp")
        .arg("-l")
        .arg("--")
        .arg(base)
        .arg(variant)
        .output()?;
    if output.status.code() != Some(1) || !output.stderr.is_empty() {
        return Err(invalid_data("cmp did not report the expected differences"));
    }
    let text = std::str::from_utf8(&output.stdout)
        .map_err(|_| invalid_data("cmp returned non-UTF-8 diagnostics"))?;
    let mut actual = BTreeMap::new();
    for line in text.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(invalid_data("unexpected cmp output"));
        }
        let position = fields[0]
            .parse::<u64>()
            .map_err(|_| invalid_data("invalid cmp offset"))?;
        let old = u8::from_str_radix(fields[1], 8)
            .map_err(|_| invalid_data("invalid cmp source byte"))?;
        let new = u8::from_str_radix(fields[2], 8)
            .map_err(|_| invalid_data("invalid cmp target byte"))?;
        actual.insert(
            position
                .checked_sub(1)
                .ok_or_else(|| invalid_data("cmp offset is not one based"))?,
            (old, new),
        );
    }
    if actual.len() != expected.len()
        || expected.iter().any(|(offset, mutation)| {
            actual.get(offset).copied() != Some((mutation.old, mutation.new))
        })
    {
        return Err(invalid_data("variant differs outside the mutation plan"));
    }
    Ok(())
}

fn sha256sum(path: &Path) -> io::Result<String> {
    let output = Command::new("sha256sum").arg("--").arg(path).output()?;
    if !output.status.success() {
        return Err(io::Error::other("sha256sum failed"));
    }
    let text = std::str::from_utf8(&output.stdout)
        .map_err(|_| invalid_data("sha256sum returned non-UTF-8 output"))?;
    let digest = text
        .split_whitespace()
        .next()
        .ok_or_else(|| invalid_data("sha256sum omitted the digest"))?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_data("sha256sum returned an invalid digest"));
    }
    Ok(digest.to_owned())
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

struct ObservedMutation {
    offset: u64,
    old: u8,
    new: u8,
    xor_mask: u8,
}

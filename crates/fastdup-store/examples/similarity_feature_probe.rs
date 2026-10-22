//! Compares Similarity candidate features on a synthetic changed-block backup.
//!
//! Usage: `similarity_feature_probe <base-image> <changed-block-percent> <shifted-block-percent> <seed>`
//!
//! The base image stands in for a protected disk. A changed-block increment is
//! built from it the way a raw incremental backup file is laid out: a sample
//! of 1-MiB blocks, each edited in a few 4-KiB pages and sometimes shifted by
//! an in-file insertion, written back to back behind a small block header.
//! Both streams are cut with the production `SeqCDC` profile. Increment Chunks
//! that are Exact duplicates are dropped; every other one is scored per
//! feature family with the production query shape: four bucket probes, at
//! most 64 representatives per bucket, the 16 best candidates, four Zstd-L3
//! prefix trials, and the 5 %/4-KiB acceptance rule.
//!
//! Feature families:
//! - `v2-band`: v1 Superfeatures in geometric length bands (today);
//! - `v1-any-length`: v1 Superfeatures without a length component;
//! - `subblock-slots`: one min-hash per slot over ~5-KiB gear sub-block digests;
//! - `subblock-bottom4`: the four smallest sub-block digests in one key space.
//!
//! It writes nothing and prints aggregate counts only.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "a probe over in-memory corpora far below 4 GiB per Chunk"
)]

use std::collections::{BTreeMap, HashMap, HashSet};

use fastdup_store::{SeqCdcConfig, seqcdc_cut};
use rayon::prelude::*;

const SEQCDC: SeqCdcConfig = SeqCdcConfig {
    sequence_length: 6,
    skip_trigger: 50,
    skip_bytes: 1_024,
    minimum_bytes: 16 * 1_024,
    maximum_bytes: 256 * 1_024,
};
const BLOCK: usize = 1 << 20;
const PAGE: usize = 4_096;
const BLOCK_HEADER: usize = 512;
const REPRESENTATIVES: usize = 64;
const CANDIDATES: usize = 16;
const TRIALS: usize = 4;
const SUBBLOCK_MINIMUM: usize = 1_024;
const SUBBLOCK_MASK: u64 = (1 << 12) - 1;
const SUBBLOCK_MAXIMUM: usize = 32 * 1_024;
const SHINGLE_BYTES: usize = 32;
const MINIMIZER_SPAN: usize = 64;
const SUPERFEATURE_SEEDS: [u64; 4] = [
    0xbb67_ae85_84ca_a73b,
    0x3c6e_f372_fe94_f82b,
    0xa54f_f53a_5f1d_36f1,
    0x510e_527f_ade6_82d1,
];
const SKETCH_SEEDS: [u64; 8] = [
    0x9b05_688c_2b3e_6c1f,
    0x1f83_d9ab_fb41_bd6b,
    0x5be0_cd19_137e_2179,
    0xcbbb_9d5d_c105_9ed8,
    0x629a_292a_367c_d507,
    0x9159_015a_3070_dd17,
    0x152f_ecd8_f70e_5939,
    0x6733_2667_ffc0_0b31,
];
const FAMILIES: [&str; 4] = [
    "v2-band",
    "v1-any-length",
    "subblock-slots",
    "subblock-bottom4",
];

const fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix64(self.0)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let word = self.next().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }
}

fn cut(bytes: &[u8]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let length = seqcdc_cut(rest, SEQCDC).clamp(1, rest.len());
        chunks.push(&rest[..length]);
        rest = &rest[length..];
    }
    chunks
}

/// v1 Superfeatures and Sketch, mirroring `SimilarityFingerprint::v1`.
fn fingerprint_v1(bytes: &[u8]) -> ([u64; 4], [u64; 8]) {
    let byte_hash = |byte: u8| mix64(0x6a09_e667_f3bc_c909 ^ u64::from(byte));
    let mut superfeatures = [u64::MAX; 4];
    let mut votes = [0_i32; 512];
    let mut commit = |minimizer: u64| {
        for (slot, seed) in superfeatures.iter_mut().zip(SUPERFEATURE_SEEDS) {
            *slot = (*slot).min(mix64(minimizer ^ seed));
        }
        for (word_index, seed) in SKETCH_SEEDS.iter().enumerate() {
            let word = mix64(minimizer ^ seed);
            for bit in 0..64 {
                votes[word_index * 64 + bit] += if word & (1 << bit) == 0 { -1 } else { 1 };
            }
        }
    };
    let mut rolling = 0_u64;
    let mut minimum = u64::MAX;
    let mut span = 0;
    for (position, &byte) in bytes.iter().enumerate() {
        rolling = rolling.rotate_left(1) ^ byte_hash(byte);
        if position >= SHINGLE_BYTES {
            rolling ^= byte_hash(bytes[position - SHINGLE_BYTES]).rotate_left(32);
        }
        if position + 1 >= SHINGLE_BYTES.min(bytes.len()) {
            minimum = minimum.min(rolling);
            span += 1;
            if span == MINIMIZER_SPAN {
                commit(minimum);
                minimum = u64::MAX;
                span = 0;
            }
        }
    }
    if span != 0 {
        commit(minimum);
    }
    let mut sketch = [0_u64; 8];
    for (index, vote) in votes.iter().enumerate() {
        if *vote > 0 {
            sketch[index / 64] |= 1 << (index % 64);
        }
    }
    (superfeatures, sketch)
}

fn gear_table() -> [u64; 256] {
    let mut table = [0_u64; 256];
    for (index, entry) in table.iter_mut().enumerate() {
        *entry = mix64(0x9e37_79b9_7f4a_7c15_u64.wrapping_mul(index as u64 + 1));
    }
    table
}

/// Digests of the content-defined sub-blocks, fill sub-blocks excluded
/// unless nothing else remains.
fn subblock_digests(bytes: &[u8], table: &[u64; 256]) -> Vec<u64> {
    let mut digests = Vec::new();
    let mut fills = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let rest = &bytes[offset..];
        let mut length = rest.len().min(SUBBLOCK_MAXIMUM);
        if rest.len() > SUBBLOCK_MINIMUM {
            let mut hash = 0_u64;
            for (index, byte) in rest.iter().enumerate().take(length).skip(SUBBLOCK_MINIMUM) {
                hash = (hash << 1).wrapping_add(table[usize::from(*byte)]);
                if hash & SUBBLOCK_MASK == 0 {
                    length = index + 1;
                    break;
                }
            }
        }
        let unit = &rest[..length];
        let digest = u64::from_le_bytes(
            blake3::hash(unit).as_bytes()[..8]
                .try_into()
                .expect("digest prefix"),
        );
        if unit.iter().all(|byte| *byte == unit[0]) {
            fills.push(digest);
        } else {
            digests.push(digest);
        }
        offset += length;
    }
    if digests.is_empty() { fills } else { digests }
}

fn band(length: usize) -> u64 {
    let length = length as u32;
    if length <= 1_024 {
        return u64::from(length);
    }
    let highest = 1_u32 << length.ilog2();
    let step = (highest / 8).max(1_024);
    u64::from(length / step * step)
}

/// Bucket key `(slot, length component, feature)` to its representatives.
type Buckets = HashMap<(u8, u64, u64), Vec<([u8; 32], usize)>>;

struct Features {
    keys: [Vec<(u8, u64, u64)>; 4],
    sketch: [u64; 8],
}

fn features(bytes: &[u8], table: &[u64; 256]) -> Features {
    let (superfeatures, sketch) = fingerprint_v1(bytes);
    let digests = subblock_digests(bytes, table);
    let mut slots = [u64::MAX; 4];
    for digest in &digests {
        for (slot, seed) in slots.iter_mut().zip(SUPERFEATURE_SEEDS) {
            *slot = (*slot).min(mix64(digest ^ seed));
        }
    }
    let mut bottom = digests.clone();
    bottom.sort_unstable();
    bottom.dedup();
    bottom.truncate(4);
    let slot_keys = |values: [u64; 4], length: u64| {
        values
            .iter()
            .enumerate()
            .map(|(slot, value)| (slot as u8, length, *value))
            .collect::<Vec<_>>()
    };
    Features {
        keys: [
            slot_keys(superfeatures, band(bytes.len())),
            slot_keys(superfeatures, 0),
            slot_keys(slots, 0),
            bottom.iter().map(|digest| (0, 0, *digest)).collect(),
        ],
        sketch,
    }
}

fn chunk_id(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

fn compress(bytes: &[u8]) -> usize {
    zstd::bulk::compress(bytes, 3)
        .expect("plain compression")
        .len()
}

/// Mirrors the production prefix codec, which pledges the target length.
fn compress_with_prefix(base: &[u8], target: &[u8], pledge: bool) -> usize {
    let mut frame = vec![0; zstd::zstd_safe::compress_bound(target.len())];
    let mut context = zstd::zstd_safe::CCtx::create();
    context
        .set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(3))
        .expect("prefix level");
    if pledge {
        context
            .set_pledged_src_size(Some(target.len() as u64))
            .expect("pledged size");
    }
    context.ref_prefix(base).expect("prefix reference");
    context
        .compress2(frame.as_mut_slice(), target)
        .expect("prefix compression")
}

fn accepted(independent: usize, dependent: usize) -> bool {
    let savings = 4_096.max((independent * 5).div_ceil(100));
    dependent + savings <= independent
}

fn increment(base: &[u8], percent: usize, shifted: usize, rng: &mut Rng) -> Vec<u8> {
    let blocks = base.len() / BLOCK;
    let mut chosen = (0..blocks)
        .filter(|_| rng.below(100) < percent)
        .collect::<Vec<_>>();
    chosen.sort_unstable();
    let mut output = Vec::with_capacity(chosen.len() * (BLOCK + BLOCK_HEADER));
    for block in chosen {
        let mut data = base[block * BLOCK..(block + 1) * BLOCK].to_vec();
        for _ in 0..=rng.below(32) {
            let page = rng.below(BLOCK / PAGE) * PAGE;
            if rng.below(2) == 0 {
                rng.fill(&mut data[page..page + PAGE]);
            } else {
                let source = rng.below(base.len() / PAGE) * PAGE;
                data[page..page + PAGE].copy_from_slice(&base[source..source + PAGE]);
            }
        }
        if rng.below(100) < shifted {
            let at = rng.below(BLOCK);
            let mut inserted = vec![0; 1 + rng.below(512)];
            rng.fill(&mut inserted);
            data.splice(at..at, inserted);
            data.truncate(BLOCK);
        }
        let mut header = [0_u8; BLOCK_HEADER];
        rng.fill(&mut header);
        output.extend_from_slice(&header);
        output.extend_from_slice(&data);
    }
    output
}

#[derive(Default)]
struct Score {
    candidates: usize,
    accepted: usize,
    physical: usize,
}

#[allow(clippy::too_many_lines, reason = "one linear probe report")]
fn main() {
    let arguments = std::env::args().collect::<Vec<_>>();
    assert!(
        arguments.len() == 5,
        "usage: similarity_feature_probe <base-image> <changed-block-percent> <shifted-block-percent> <seed>"
    );
    let base = std::fs::read(&arguments[1]).expect("read base image");
    let percent = arguments[2].parse::<usize>().expect("percent");
    let shifted = arguments[3].parse::<usize>().expect("shifted percent");
    let mut rng = Rng(arguments[4].parse::<u64>().expect("seed"));
    let table = gear_table();
    let target_stream = increment(&base, percent, shifted, &mut rng);

    let references = cut(&base);
    let reference_features = references
        .par_iter()
        .map(|chunk| (chunk_id(chunk), features(chunk, &table)))
        .collect::<Vec<_>>();
    let mut known = HashSet::new();
    let mut buckets: [Buckets; 4] = Default::default();
    for (ordinal, (id, features)) in reference_features.iter().enumerate() {
        if !known.insert(*id) {
            continue;
        }
        for (family, keys) in features.keys.iter().enumerate() {
            for key in keys {
                buckets[family]
                    .entry(*key)
                    .or_default()
                    .push((*id, ordinal));
            }
        }
    }
    for family in &mut buckets {
        for representatives in family.values_mut() {
            representatives.sort_unstable();
            representatives.dedup();
            representatives.truncate(REPRESENTATIVES);
        }
    }

    let mut new_chunks = Vec::new();
    let mut seen = HashSet::new();
    let targets = cut(&target_stream);
    for chunk in &targets {
        let id = chunk_id(chunk);
        if !known.contains(&id) && seen.insert(id) {
            new_chunks.push(*chunk);
        }
    }
    // Production ranks by Sketch distance alone; the default ranks by the
    // number of matching buckets first.
    let sketch_only = std::env::var_os("FASTDUP_PROBE_SKETCH_ONLY").is_some();
    let pledge = std::env::var_os("FASTDUP_PROBE_NO_PLEDGE").is_none();
    // Optional bound on Base length relative to the target, for restore reads.
    let ratio_cap = std::env::var("FASTDUP_PROBE_RATIO_CAP")
        .ok()
        .map(|value| value.parse::<usize>().expect("ratio cap"));
    let results = new_chunks
        .par_iter()
        .map(|chunk| {
            let query = features(chunk, &table);
            let plain = compress(chunk);
            let mut physical = [plain; 4];
            let mut found = [false; 4];
            for family in 0..4 {
                let mut hits: BTreeMap<usize, usize> = BTreeMap::new();
                for key in &query.keys[family] {
                    for (_, ordinal) in buckets[family].get(key).into_iter().flatten() {
                        *hits.entry(*ordinal).or_default() += 1;
                    }
                }
                let mut ranked = hits
                    .into_iter()
                    .filter(|(ordinal, _)| {
                        ratio_cap.is_none_or(|cap| references[*ordinal].len() <= cap * chunk.len())
                    })
                    .map(|(ordinal, count)| {
                        let distance = reference_features[ordinal]
                            .1
                            .sketch
                            .iter()
                            .zip(query.sketch)
                            .map(|(left, right)| (left ^ right).count_ones())
                            .sum::<u32>();
                        let count = if sketch_only { 0 } else { count };
                        (std::cmp::Reverse(count), distance, ordinal)
                    })
                    .collect::<Vec<_>>();
                ranked.sort_unstable();
                ranked.truncate(CANDIDATES);
                found[family] = !ranked.is_empty();
                for (_, _, ordinal) in ranked.into_iter().take(TRIALS) {
                    let dependent = compress_with_prefix(references[ordinal], chunk, pledge) + 80;
                    if accepted(plain, dependent) {
                        physical[family] = physical[family].min(dependent);
                    }
                }
            }
            (chunk.len(), plain, found, physical)
        })
        .collect::<Vec<_>>();

    let logical = results.iter().map(|r| r.0).sum::<usize>();
    let plain = results.iter().map(|r| r.1).sum::<usize>();
    println!(
        "base_MiB={} increment_MiB={} reference_chunks={} increment_chunks={} new_chunks={} new_MiB={:.1} plain={:.1}%",
        base.len() >> 20,
        target_stream.len() >> 20,
        references.len(),
        targets.len(),
        new_chunks.len(),
        logical as f64 / 1_048_576.0,
        100.0 * plain as f64 / logical as f64,
    );
    for (family, name) in FAMILIES.iter().enumerate() {
        let mut score = Score::default();
        for (_, independent, found, physical) in &results {
            score.candidates += usize::from(found[family]);
            score.accepted += usize::from(physical[family] < *independent);
            score.physical += physical[family];
        }
        println!(
            "family={name:<17} candidates={:.1}% accepted={:.1}% physical={:.1}% vs_plain={:+.1}%",
            100.0 * score.candidates as f64 / results.len() as f64,
            100.0 * score.accepted as f64 / results.len() as f64,
            100.0 * score.physical as f64 / logical as f64,
            100.0 * (score.physical as f64 / plain as f64 - 1.0),
        );
    }
}

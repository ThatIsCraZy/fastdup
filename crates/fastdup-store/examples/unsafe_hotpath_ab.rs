//! A/B microbenchmarks for unsafe candidates on read and write hot paths.
//!
//! These challengers deliberately duplicate only the small kernels under
//! test. They do not replace field-wise durable serialization in production.

#![allow(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_ptr_alignment
)]

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::hint::black_box;
use std::ptr::NonNull;
use std::time::{Duration, Instant};

use memmap2::{Advice, MmapMut};

const PAGE: usize = 4096;
const ENTRY: usize = 128;
const ENTRIES: usize = PAGE / ENTRY;
const SAMPLES: usize = 11;

fn main() {
    let mut page = [0_u8; PAGE];
    for (index, byte) in page.iter_mut().enumerate() {
        *byte = (index as u8).wrapping_mul(37).wrapping_add(11);
    }
    assert_eq!(read_fields_safe(&page), read_fields_unsafe(&page));

    let read_iters = 2_000_000;
    report_pair(
        "field-read-4k-page",
        read_iters,
        || black_box(read_fields_safe(black_box(&page))),
        || black_box(read_fields_unsafe(black_box(&page))),
    );

    let write_iters = 1_000_000;
    let mut safe_page = [0_u8; PAGE];
    let mut unsafe_page = [0_u8; PAGE];
    write_fields_safe(&mut safe_page, 7);
    write_fields_unsafe(&mut unsafe_page, 7);
    assert_eq!(safe_page, unsafe_page);
    report_pair_mut(
        "field-write-4k-page",
        write_iters,
        || {
            write_fields_safe(black_box(&mut safe_page), black_box(19));
            black_box(safe_page[777])
        },
        || {
            write_fields_unsafe(black_box(&mut unsafe_page), black_box(19));
            black_box(unsafe_page[777])
        },
    );

    let source = fixture(256 * 1024);
    let mut safe_copy = vec![0_u8; source.len()];
    let mut unsafe_copy = vec![0_u8; source.len()];
    let copy_iters = 20_000;
    report_pair_mut(
        "payload-copy-256k",
        copy_iters,
        || {
            safe_copy.copy_from_slice(black_box(&source));
            black_box(safe_copy[12345])
        },
        || {
            copy_unsafe(black_box(&mut unsafe_copy), black_box(&source));
            black_box(unsafe_copy[12345])
        },
    );
    assert_eq!(safe_copy, unsafe_copy);

    for length in [128 * 1024, 4 * 1024 * 1024, 32 * 1024 * 1024] {
        let payload = fixture(length - 2 * PAGE);
        let iters = (512 * 1024 * 1024 / length).max(16);
        let current = build_zero_then_copy(&payload);
        let safe = build_safe_append(&payload);
        let unsafe_image = build_unsafe_uninit(&payload);
        assert_eq!(current.image(), safe.image());
        assert_eq!(current.image(), unsafe_image.as_slice());

        report_triple(
            &format!("aligned-image-{}k", length / 1024),
            iters,
            || {
                black_box(build_zero_then_copy(black_box(&payload)))
                    .image()
                    .len()
            },
            || {
                black_box(build_safe_append(black_box(&payload)))
                    .image()
                    .len()
            },
            || black_box(build_unsafe_uninit(black_box(&payload))).len(),
        );
    }

    benchmark_long_lived_arena();
}

#[inline(never)]
fn read_fields_safe(bytes: &[u8; PAGE]) -> u64 {
    let mut sum = 0_u64;
    for ordinal in 0..ENTRIES {
        let base = ordinal * ENTRY;
        sum = sum.wrapping_add(u64::from_le_bytes(
            bytes[base + 32..base + 40].try_into().unwrap(),
        ));
        sum ^= u64::from(u32::from_le_bytes(
            bytes[base + 56..base + 60].try_into().unwrap(),
        ));
        sum = sum.rotate_left(9)
            ^ u64::from_le_bytes(bytes[base + 88..base + 96].try_into().unwrap());
    }
    sum
}

#[inline(never)]
fn read_fields_unsafe(bytes: &[u8; PAGE]) -> u64 {
    let mut sum = 0_u64;
    let pointer = bytes.as_ptr();
    for ordinal in 0..ENTRIES {
        let base = ordinal * ENTRY;
        // SAFETY: every fixed offset is within the 4-KiB page; unaligned loads
        // accept the byte alignment of durable field encodings.
        unsafe {
            sum = sum.wrapping_add(
                pointer
                    .add(base + 32)
                    .cast::<u64>()
                    .read_unaligned()
                    .to_le(),
            );
            sum ^= u64::from(
                pointer
                    .add(base + 56)
                    .cast::<u32>()
                    .read_unaligned()
                    .to_le(),
            );
            sum = sum.rotate_left(9)
                ^ pointer
                    .add(base + 88)
                    .cast::<u64>()
                    .read_unaligned()
                    .to_le();
        }
    }
    sum
}

#[inline(never)]
fn write_fields_safe(bytes: &mut [u8; PAGE], seed: u64) {
    for ordinal in 0..ENTRIES {
        let base = ordinal * ENTRY;
        let value = seed
            .wrapping_add(ordinal as u64)
            .rotate_left(ordinal as u32);
        bytes[base + 32..base + 40].copy_from_slice(&value.to_le_bytes());
        bytes[base + 56..base + 60].copy_from_slice(&(value as u32).to_le_bytes());
        bytes[base + 88..base + 96].copy_from_slice(&value.wrapping_mul(17).to_le_bytes());
    }
}

#[inline(never)]
fn write_fields_unsafe(bytes: &mut [u8; PAGE], seed: u64) {
    let pointer = bytes.as_mut_ptr();
    for ordinal in 0..ENTRIES {
        let base = ordinal * ENTRY;
        let value = seed
            .wrapping_add(ordinal as u64)
            .rotate_left(ordinal as u32);
        // SAFETY: every fixed offset is within the 4-KiB output page and the
        // written fields do not overlap.
        unsafe {
            pointer
                .add(base + 32)
                .cast::<u64>()
                .write_unaligned(value.to_le());
            pointer
                .add(base + 56)
                .cast::<u32>()
                .write_unaligned((value as u32).to_le());
            pointer
                .add(base + 88)
                .cast::<u64>()
                .write_unaligned(value.wrapping_mul(17).to_le());
        }
    }
}

#[inline(never)]
fn copy_unsafe(destination: &mut [u8], source: &[u8]) {
    assert_eq!(destination.len(), source.len());
    // SAFETY: equal slice lengths prove the complete ranges are valid and the
    // independently allocated benchmark buffers cannot overlap.
    unsafe {
        std::ptr::copy_nonoverlapping(source.as_ptr(), destination.as_mut_ptr(), source.len());
    }
}

fn fixture(length: usize) -> Vec<u8> {
    let mut state = 0x243f_6a88_85a3_08d3_u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn build_zero_then_copy(payload: &[u8]) -> SafeAlignedImage {
    let length = payload.len() + 2 * PAGE;
    let mut allocation = vec![0_u8; length + PAGE - 1];
    let start = (PAGE - allocation.as_ptr().addr() % PAGE) % PAGE;
    allocation[start + PAGE..start + PAGE + payload.len()].copy_from_slice(payload);
    SafeAlignedImage {
        allocation,
        start,
        length,
    }
}

struct SafeAlignedImage {
    allocation: Vec<u8>,
    start: usize,
    length: usize,
}

impl SafeAlignedImage {
    fn image(&self) -> &[u8] {
        &self.allocation[self.start..self.start + self.length]
    }
}

fn build_safe_append(payload: &[u8]) -> SafeAlignedImage {
    let length = payload.len() + 2 * PAGE;
    let mut allocation: Vec<u8> = Vec::with_capacity(length + PAGE - 1);
    let start = (PAGE - allocation.as_ptr().addr() % PAGE) % PAGE;
    allocation.resize(start + PAGE, 0);
    allocation.extend_from_slice(payload);
    allocation.resize(start + length, 0);
    SafeAlignedImage {
        allocation,
        start,
        length,
    }
}

struct UnsafeAlignedImage {
    pointer: NonNull<u8>,
    layout: Layout,
}

impl UnsafeAlignedImage {
    fn as_slice(&self) -> &[u8] {
        // SAFETY: the allocation is live for `layout.size()` initialized bytes.
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.layout.size()) }
    }
    fn len(&self) -> usize {
        self.layout.size()
    }
}

impl Drop for UnsafeAlignedImage {
    fn drop(&mut self) {
        // SAFETY: this exact pointer was allocated with this exact layout.
        unsafe { dealloc(self.pointer.as_ptr(), self.layout) }
    }
}

fn build_unsafe_uninit(payload: &[u8]) -> UnsafeAlignedImage {
    let length = payload.len() + 2 * PAGE;
    let layout = Layout::from_size_align(length, PAGE).unwrap();
    // SAFETY: valid nonzero layout; null is handled as allocation failure.
    let raw = unsafe { alloc(layout) };
    let pointer = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));
    // SAFETY: the allocation owns `length` writable bytes. Header and footer
    // are zeroed and the disjoint body is completely initialized from payload.
    unsafe {
        pointer.as_ptr().write_bytes(0, PAGE);
        std::ptr::copy_nonoverlapping(payload.as_ptr(), pointer.as_ptr().add(PAGE), payload.len());
        pointer
            .as_ptr()
            .add(PAGE + payload.len())
            .write_bytes(0, PAGE);
    }
    UnsafeAlignedImage { pointer, layout }
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct ProbeBlock([u64; 8]);

fn benchmark_long_lived_arena() {
    const BYTES: usize = 128 * 1024 * 1024;
    const BLOCKS: usize = BYTES / std::mem::size_of::<ProbeBlock>();
    const QUERIES: usize = 4_000_000;

    let mut heap = vec![ProbeBlock::default(); BLOCKS];
    for (ordinal, block) in heap.iter_mut().enumerate() {
        block.0[0] = ordinal as u64;
    }

    let mut mapping = MmapMut::map_anon(BYTES).expect("anonymous benchmark mapping allocates");
    mapping
        .advise(Advice::HugePage)
        .expect("benchmark host accepts huge-page advice");
    assert_eq!(
        mapping.as_ptr().addr() % std::mem::align_of::<ProbeBlock>(),
        0
    );
    // SAFETY: the mapping contains exactly `BLOCKS` suitably aligned blocks.
    // Every block is initialized before the mapping is read.
    let mapped = unsafe {
        let blocks = std::slice::from_raw_parts_mut(mapping.as_mut_ptr().cast(), BLOCKS);
        for (ordinal, block) in blocks.iter_mut().enumerate() {
            std::ptr::write(block, ProbeBlock([ordinal as u64, 0, 0, 0, 0, 0, 0, 0]));
        }
        std::slice::from_raw_parts(mapping.as_ptr().cast::<ProbeBlock>(), BLOCKS)
    };

    let mut state = 0x1319_8a2e_0370_7344_u64;
    let queries = (0..QUERIES)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            usize::try_from(state & u64::try_from(BLOCKS - 1).unwrap()).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        probe_blocks(&heap, &queries),
        probe_blocks(mapped, &queries)
    );
    println!(
        "long-lived-arena-residency heap_anon_huge_kib={} mapped_anon_huge_kib={}",
        anon_huge_kib(heap.as_ptr().addr()),
        anon_huge_kib(mapped.as_ptr().addr()),
    );
    report_pair(
        "long-lived-arena-random-probe-128m",
        1,
        || black_box(probe_blocks(black_box(&heap), black_box(&queries))),
        || black_box(probe_blocks(black_box(mapped), black_box(&queries))),
    );
}

fn anon_huge_kib(address: usize) -> u64 {
    let Ok(smaps) = std::fs::read_to_string("/proc/self/smaps") else {
        return 0;
    };
    let mut contains_address = false;
    for line in smaps.lines() {
        if let Some((range, _)) = line.split_once(' ')
            && let Some((start, end)) = range.split_once('-')
            && let (Ok(start), Ok(end)) = (
                usize::from_str_radix(start, 16),
                usize::from_str_radix(end, 16),
            )
        {
            contains_address = start <= address && address < end;
            continue;
        }
        if contains_address
            && let Some(value) = line.strip_prefix("AnonHugePages:")
            && let Some(kib) = value.split_ascii_whitespace().next()
        {
            return kib.parse().unwrap_or(0);
        }
    }
    0
}

#[inline(never)]
fn probe_blocks(blocks: &[ProbeBlock], queries: &[usize]) -> u64 {
    let mut checksum = 0_u64;
    for ordinal in queries {
        checksum = checksum.rotate_left(7) ^ blocks[*ordinal].0[0];
    }
    checksum
}

fn measure(mut operation: impl FnMut()) -> Duration {
    let started = Instant::now();
    operation();
    started.elapsed()
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn report_pair<T>(
    label: &str,
    iterations: usize,
    mut safe: impl FnMut() -> T,
    mut unsafe_: impl FnMut() -> T,
) {
    let mut safe_samples = Vec::with_capacity(SAMPLES);
    let mut unsafe_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let first_safe = sample % 2 == 0;
        if first_safe {
            safe_samples.push(measure(|| {
                for _ in 0..iterations {
                    black_box(safe());
                }
            }));
            unsafe_samples.push(measure(|| {
                for _ in 0..iterations {
                    black_box(unsafe_());
                }
            }));
        } else {
            unsafe_samples.push(measure(|| {
                for _ in 0..iterations {
                    black_box(unsafe_());
                }
            }));
            safe_samples.push(measure(|| {
                for _ in 0..iterations {
                    black_box(safe());
                }
            }));
        }
    }
    print_pair(
        label,
        iterations,
        median(safe_samples),
        median(unsafe_samples),
    );
}

fn report_pair_mut<T>(
    label: &str,
    iterations: usize,
    safe: impl FnMut() -> T,
    unsafe_: impl FnMut() -> T,
) {
    report_pair(label, iterations, safe, unsafe_);
}

fn print_pair(label: &str, iterations: usize, safe: Duration, unsafe_: Duration) {
    println!(
        "{label} iterations={iterations} safe_ns_per_op={:.3} unsafe_ns_per_op={:.3} unsafe_speedup={:.4}x",
        safe.as_secs_f64() * 1e9 / iterations as f64,
        unsafe_.as_secs_f64() * 1e9 / iterations as f64,
        safe.as_secs_f64() / unsafe_.as_secs_f64(),
    );
}

fn report_triple<T>(
    label: &str,
    iterations: usize,
    mut current: impl FnMut() -> T,
    mut safe: impl FnMut() -> T,
    mut unsafe_: impl FnMut() -> T,
) {
    let sample = |operation: &mut dyn FnMut() -> T| {
        measure(|| {
            for _ in 0..iterations {
                black_box(operation());
            }
        })
    };
    let mut current_samples = Vec::with_capacity(SAMPLES);
    let mut safe_samples = Vec::with_capacity(SAMPLES);
    let mut unsafe_samples = Vec::with_capacity(SAMPLES);
    for ordinal in 0..SAMPLES {
        match ordinal % 3 {
            0 => {
                current_samples.push(sample(&mut current));
                safe_samples.push(sample(&mut safe));
                unsafe_samples.push(sample(&mut unsafe_));
            }
            1 => {
                safe_samples.push(sample(&mut safe));
                unsafe_samples.push(sample(&mut unsafe_));
                current_samples.push(sample(&mut current));
            }
            _ => {
                unsafe_samples.push(sample(&mut unsafe_));
                current_samples.push(sample(&mut current));
                safe_samples.push(sample(&mut safe));
            }
        }
    }
    let current = median(current_samples);
    let safe = median(safe_samples);
    let unsafe_ = median(unsafe_samples);
    println!(
        "{label} iterations={iterations} current_ns_per_op={:.3} safe_append_ns_per_op={:.3} unsafe_uninit_ns_per_op={:.3} safe_speedup={:.4}x unsafe_speedup={:.4}x unsafe_vs_safe={:.4}x",
        current.as_secs_f64() * 1e9 / iterations as f64,
        safe.as_secs_f64() * 1e9 / iterations as f64,
        unsafe_.as_secs_f64() * 1e9 / iterations as f64,
        current.as_secs_f64() / safe.as_secs_f64(),
        current.as_secs_f64() / unsafe_.as_secs_f64(),
        safe.as_secs_f64() / unsafe_.as_secs_f64(),
    );
}

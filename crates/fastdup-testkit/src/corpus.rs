use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

const BASE_RECORDS: u32 = 3_200;
const V3_ADDITIONS: u32 = 24;
const MAX_MUTATION_GUARD_BYTES: u64 = 1_024 * 1_024;

/// One deterministic byte change for a minimally modified large-file fixture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteMutation {
    /// Absolute byte offset in the source file.
    pub offset: u64,
    /// Nonzero mask combined with the original byte using bitwise XOR.
    pub xor_mask: u8,
}

/// Builds reproducible, distinct minimal-mutation plans for large-file variants.
///
/// Offsets are unique within each variant and exclude a bounded edge guard so
/// mutations are distributed through file content rather than concentrated in
/// leading or trailing format metadata.
///
/// # Errors
///
/// Returns `InvalidInput` when the file or editable span cannot contain the
/// requested number of unique edits.
pub fn minimal_variant_plan(
    file_length: u64,
    variant_count: usize,
    edits_per_variant: usize,
    seed: u64,
) -> io::Result<Vec<Vec<ByteMutation>>> {
    let guard = (file_length / 16).min(MAX_MUTATION_GUARD_BYTES);
    let editable_length = file_length
        .checked_sub(guard.saturating_mul(2))
        .ok_or_else(invalid_mutation_plan)?;
    let edits_u64 = u64::try_from(edits_per_variant).map_err(|_| invalid_mutation_plan())?;
    if editable_length == 0 || edits_u64 > editable_length {
        return Err(invalid_mutation_plan());
    }

    let mut state = if seed == 0 {
        0x9e37_79b9_7f4a_7c15
    } else {
        seed
    };
    let mut variants = Vec::with_capacity(variant_count);
    for _ in 0..variant_count {
        let mut offsets = BTreeSet::new();
        while offsets.len() < edits_per_variant {
            offsets.insert(guard + next_random(&mut state) % editable_length);
        }
        let mut mutations = Vec::with_capacity(edits_per_variant);
        for offset in offsets {
            let candidate = next_random(&mut state).to_le_bytes()[0];
            mutations.push(ByteMutation {
                offset,
                xor_mask: candidate.max(1),
            });
        }
        variants.push(mutations);
    }
    Ok(variants)
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state >> 12;
    *state ^= *state << 25;
    *state ^= *state >> 27;
    state.wrapping_mul(0x2545_f491_4f6c_dd1d)
}

fn invalid_mutation_plan() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "file has too few editable offsets for the requested mutation plan",
    )
}

/// Writes three related JSON and XML versions into a new or empty directory.
///
/// The byte sequence is independent of host time, randomness, and iteration
/// order. Files are created without replacement so a prior golden corpus is
/// never silently changed.
///
/// # Errors
///
/// Returns directory, create, write, flush, or synchronization errors.
pub fn generate_structured_corpus(root: impl AsRef<Path>) -> io::Result<()> {
    let root = root.as_ref();
    std::fs::create_dir_all(root)?;
    for version in 1..=3 {
        let ids = fixture_ids(version);
        write_json(
            &root.join(format!("inventory-v{version}.json")),
            version,
            &ids,
        )?;
        write_xml(
            &root.join(format!("inventory-v{version}.xml")),
            version,
            &ids,
        )?;
    }
    File::open(root)?.sync_all()
}

fn fixture_ids(version: u32) -> Vec<u32> {
    let end = if version == 3 {
        BASE_RECORDS + V3_ADDITIONS
    } else {
        BASE_RECORDS
    };
    (0..end)
        .filter(|id| version != 3 || *id >= BASE_RECORDS || !id.is_multiple_of(487))
        .collect()
}

fn write_json(path: &Path, version: u32, ids: &[u32]) -> io::Result<()> {
    let mut writer = fixture_writer(path)?;
    writeln!(writer, "[")?;
    for (ordinal, id) in ids.iter().copied().enumerate() {
        let fields = fields(version, id);
        let comma = if ordinal + 1 == ids.len() { "" } else { "," };
        writeln!(
            writer,
            "  {{\"id\":{id},\"vm\":\"vm-{id:06}\",\"tenant\":{},\"path\":\"/backup/tenant-{:03}/vm-{id:06}/disk-0.raw\",\"revision\":{},\"state\":\"{}\",\"payload\":\"{:016x}{:016x}{:016x}\"}}{comma}",
            fields.tenant,
            fields.tenant,
            fields.revision,
            fields.state,
            fields.payload,
            fields.payload.rotate_left(17),
            fields.payload.rotate_right(11),
        )?;
    }
    writeln!(writer, "]")?;
    finish(writer)
}

fn write_xml(path: &Path, version: u32, ids: &[u32]) -> io::Result<()> {
    let mut writer = fixture_writer(path)?;
    writeln!(writer, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")?;
    writeln!(writer, "<inventory version=\"{version}\">")?;
    for id in ids.iter().copied() {
        let fields = fields(version, id);
        writeln!(
            writer,
            "  <machine id=\"{id}\" vm=\"vm-{id:06}\" tenant=\"{}\" revision=\"{}\" state=\"{}\"><path>/backup/tenant-{:03}/vm-{id:06}/disk-0.raw</path><payload>{:016x}{:016x}{:016x}</payload></machine>",
            fields.tenant,
            fields.revision,
            fields.state,
            fields.tenant,
            fields.payload,
            fields.payload.rotate_left(17),
            fields.payload.rotate_right(11),
        )?;
    }
    writeln!(writer, "</inventory>")?;
    finish(writer)
}

fn fixture_writer(path: &Path) -> io::Result<BufWriter<File>> {
    Ok(BufWriter::new(
        OpenOptions::new().create_new(true).write(true).open(path)?,
    ))
}

fn finish(mut writer: BufWriter<File>) -> io::Result<()> {
    writer.flush()?;
    writer.get_ref().sync_all()
}

fn fields(version: u32, id: u32) -> FixtureFields {
    let changed = id.is_multiple_of(37) || (version == 3 && id.is_multiple_of(113));
    let revision = id % 23 + u32::from(changed) * version * 1_000;
    let state = if changed { "changed" } else { "stable" };
    let payload = u64::from(id)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left((id % 63) + 1)
        ^ u64::from(version == 3 && id.is_multiple_of(113));
    FixtureFields {
        tenant: id.wrapping_mul(17) % 211,
        revision,
        state,
        payload,
    }
}

struct FixtureFields {
    tenant: u32,
    revision: u32,
    state: &'static str,
    payload: u64,
}

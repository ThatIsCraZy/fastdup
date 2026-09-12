//! Alignment-safe repository I/O. No buffered fallback, file mapping, or
//! page-cache advice. The aligned v1 storage envelope keeps logical length
//! separate from physical EOF, avoiding XFS buffered truncate-tail zeroing.
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

const QUANTUM: usize = 1024 * 1024;
const BLOCK: usize = 4096;
const PAYLOAD_OFFSET: u64 = 2 * BLOCK as u64;
const MAGIC: &[u8; 8] = b"FDIO0001";

#[cfg(test)]
thread_local! {
    pub(crate) static READ_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Layout {
    pub(crate) length: u64,
    generation: u64,
    slot: usize,
    wrapped: bool,
}

fn header(generation: u64, length: u64) -> [u8; BLOCK] {
    let mut bytes = [0; BLOCK];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..16].copy_from_slice(&generation.to_le_bytes());
    bytes[16..24].copy_from_slice(&length.to_le_bytes());
    let crc = crc32c::crc32c(&bytes);
    bytes[24..28].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_header(bytes: &[u8], physical: u64, slot: usize) -> Option<Layout> {
    if bytes.len() != BLOCK || &bytes[..8] != MAGIC || bytes[28..].iter().any(|byte| *byte != 0) {
        return None;
    }
    let mut checked = [0; BLOCK];
    checked.copy_from_slice(bytes);
    let crc = u32::from_le_bytes(checked[24..28].try_into().ok()?);
    checked[24..28].fill(0);
    if crc32c::crc32c(&checked) != crc {
        return None;
    }
    let generation = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
    let length = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
    if generation == 0 || length > physical.checked_sub(PAYLOAD_OFFSET)? {
        return None;
    }
    Some(Layout {
        length,
        generation,
        slot,
        wrapped: true,
    })
}

pub(crate) fn layout(file: &File) -> io::Result<Layout> {
    let physical = file.metadata()?.len();
    if physical < PAYLOAD_OFFSET || !physical.is_multiple_of(BLOCK as u64) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "repository requires aligned FDIO0001 storage; rebuild an older repository",
        ));
    }
    let bytes = read_physical(file, 0, 2 * BLOCK)?;
    let first = decode_header(&bytes[..BLOCK], physical, 0);
    let second = decode_header(&bytes[BLOCK..], physical, 1);
    match (first, second) {
        (Some(a), Some(b)) if a.generation == b.generation && a.length != b.length => {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "conflicting storage length heads",
            ))
        }
        (Some(a), Some(b)) => Ok(if a.generation > b.generation { a } else { b }),
        (Some(head), None) | (None, Some(head)) => Ok(head),
        (None, None) => {
            // The owned io_uring publisher already emits aligned Containers.
            // Their self-describing Header/Footer remain the native envelope.
            let container =
                fastdup_format::ContainerHeader::decode(&bytes[..BLOCK]).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "no valid storage length head")
                })?;
            if container.layout().file_length != physical {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native Container length mismatch",
                ));
            }
            Ok(Layout {
                length: physical,
                generation: 0,
                slot: 0,
                wrapped: false,
            })
        }
    }
}

pub(crate) fn initialize(file: &File) -> io::Result<Layout> {
    // Only a newly created name reaches here. Both initial heads are identical;
    // subsequent updates alternate slots and never overwrite the current head.
    write_physical(file, 0, &header(1, 0))?;
    write_physical(file, BLOCK as u64, &header(1, 0))?;
    Ok(Layout {
        length: 0,
        generation: 1,
        slot: 0,
        wrapped: true,
    })
}

pub(crate) fn object_len(file: &File) -> io::Result<u64> {
    Ok(layout(file)?.length)
}

#[cfg(test)]
pub(crate) fn read(file: &File, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    read_with_layout(file, layout(file)?, offset, length)
}

pub(crate) fn read_with_layout(
    file: &File,
    layout: Layout,
    offset: u64,
    length: usize,
) -> io::Result<Vec<u8>> {
    let end = offset
        .checked_add(length as u64)
        .ok_or(io::ErrorKind::InvalidInput)?;
    if end > layout.length {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let physical = offset
        .checked_add(if layout.wrapped { PAYLOAD_OFFSET } else { 0 })
        .ok_or(io::ErrorKind::InvalidInput)?;
    read_physical(file, physical, length)
}

fn set_head(file: &File, previous: Layout, length: u64) -> io::Result<Layout> {
    let generation = previous
        .generation
        .checked_add(1)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let slot = 1 - previous.slot;
    write_physical(
        file,
        slot as u64 * BLOCK as u64,
        &header(generation, length),
    )?;
    // Make this body/head pair durable before a following mutation may reuse
    // its predecessor slot. Multiple writes between outer sync_file calls
    // must never overwrite both previously durable length heads.
    file.sync_data()?;
    Ok(Layout {
        length,
        generation,
        slot,
        wrapped: true,
    })
}

fn zero(file: &File, start: u64, end: u64) -> io::Result<()> {
    let zeros = vec![0; QUANTUM];
    let mut position = start;
    while position < end {
        let length = usize::try_from((end - position).min(QUANTUM as u64))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        write_physical(file, PAYLOAD_OFFSET + position, &zeros[..length])?;
        position += length as u64;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn write(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    write_with_layout(file, layout(file)?, offset, bytes).map(|_| ())
}

pub(crate) fn write_with_layout(
    file: &File,
    previous: Layout,
    offset: u64,
    bytes: &[u8],
) -> io::Result<Layout> {
    if bytes.is_empty() {
        return Ok(previous);
    }
    let end = offset
        .checked_add(bytes.len() as u64)
        .ok_or(io::ErrorKind::InvalidInput)?;
    if !previous.wrapped {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "native published Container is immutable",
        ));
    }
    if offset > previous.length {
        zero(file, previous.length, offset)?;
    }
    write_physical(
        file,
        PAYLOAD_OFFSET
            .checked_add(offset)
            .ok_or(io::ErrorKind::InvalidInput)?,
        bytes,
    )?;
    if end > previous.length {
        return set_head(file, previous, end);
    }
    Ok(previous)
}

pub(crate) fn set_len_with_layout(
    file: &File,
    previous: Layout,
    length: u64,
) -> io::Result<Layout> {
    if !previous.wrapped {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "native published Container is immutable",
        ));
    }
    if length == previous.length {
        return Ok(previous);
    }
    if length > previous.length {
        zero(file, previous.length, length)?;
    }
    // No syscall ever sees a nonaligned physical EOF. Zero the newly hidden
    // suffix so a later extension cannot reveal a pre-truncate logical byte.
    let rounded = length
        .checked_add(BLOCK as u64 - 1)
        .ok_or(io::ErrorKind::InvalidInput)?
        / BLOCK as u64
        * BLOCK as u64;
    if length < previous.length && length < rounded {
        zero(file, length, rounded)?;
    }
    let next = set_head(file, previous, length)?;
    file.set_len(
        PAYLOAD_OFFSET
            .checked_add(rounded)
            .ok_or(io::ErrorKind::InvalidInput)?,
    )?;
    Ok(next)
}

pub(crate) fn open(path: &Path, write: bool) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

pub(crate) fn validate_filesystem(file: &File) -> io::Result<()> {
    let filesystem = rustix::fs::fstatfs(file)?;
    if filesystem.f_bsize != 4096 || filesystem.f_type != 0x5846_5342 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "repository Direct I/O requires XFS with 4096-byte blocks",
        ));
    }
    Ok(())
}

pub(crate) fn write_control_record(file: &File, bytes: &[u8]) -> io::Result<()> {
    if bytes.len() > BLOCK || !rustix::fs::fcntl_getfl(file)?.contains(rustix::fs::OFlags::DIRECT) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let mut record = [0; BLOCK];
    record[..bytes.len()].copy_from_slice(bytes);
    write_physical(file, 0, &record)?;
    file.set_len(BLOCK as u64)
}

#[derive(Clone, Copy)]
struct Alignment {
    memory: usize,
    offset: usize,
}

impl Alignment {
    fn read(file: &File) -> io::Result<Self> {
        let stat = rustix::fs::statx(
            file,
            "",
            rustix::fs::AtFlags::EMPTY_PATH,
            rustix::fs::StatxFlags::DIOALIGN,
        )?;
        let memory = stat.stx_dio_mem_align as usize;
        let offset = stat.stx_dio_offset_align as usize;
        let filesystem = rustix::fs::fstatfs(file)?;
        if stat.stx_mask & rustix::fs::StatxFlags::DIOALIGN.bits() == 0
            || !memory.is_power_of_two()
            || !offset.is_power_of_two()
            || memory > BLOCK
            || offset > BLOCK
            || filesystem.f_bsize != 4096
            || filesystem.f_type != 0x5846_5342
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "repository requires Direct I/O and 4096-byte filesystem blocks; buffered fallback is disabled",
            ));
        }
        Ok(Self { memory, offset })
    }
}

struct Buffer {
    bytes: Vec<u8>,
    start: usize,
    length: usize,
}
impl Buffer {
    fn new(length: usize, alignment: usize) -> io::Result<Self> {
        let size = length
            .checked_add(alignment - 1)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| io::ErrorKind::OutOfMemory)?;
        bytes.resize(size, 0);
        let start = (alignment - bytes.as_ptr().addr() % alignment) % alignment;
        Ok(Self {
            bytes,
            start,
            length,
        })
    }
    fn bytes(&self) -> &[u8] {
        &self.bytes[self.start..self.start + self.length]
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[self.start..self.start + self.length]
    }
}

fn read_aligned(
    file: &File,
    bytes: &mut [u8],
    offset: u64,
    alignment: Alignment,
    minimum: usize,
) -> io::Result<()> {
    let mut done = 0;
    while done < minimum {
        let count = match file.read_at(&mut bytes[done..], offset + done as u64) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        #[cfg(test)]
        READ_BYTES.with(|total| total.set(total.get() + count));
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        done += count;
        // A final EOF short read is valid if the requested logical bytes are
        // complete. A nonaligned partial read cannot be resubmitted directly.
        if done < minimum
            && (!done.is_multiple_of(alignment.offset) || !done.is_multiple_of(alignment.memory))
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unaligned short Direct-I/O read",
            ));
        }
    }
    Ok(())
}

fn read_physical(file: &File, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    let end = offset
        .checked_add(length as u64)
        .ok_or(io::ErrorKind::InvalidInput)?;
    if end > file.metadata()?.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| io::ErrorKind::OutOfMemory)?;
    if length == 0 {
        return Ok(output);
    }
    let alignment = Alignment::read(file)?;
    let mut position = offset;
    while position < end {
        let amount = usize::try_from((end - position).min(QUANTUM as u64))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        let start = position / alignment.offset as u64 * alignment.offset as u64;
        let skip = usize::try_from(position - start).map_err(|_| io::ErrorKind::InvalidInput)?;
        let size = (skip + amount).div_ceil(alignment.offset) * alignment.offset;
        let mut buffer = Buffer::new(size, alignment.memory)?;
        read_aligned(file, buffer.bytes_mut(), start, alignment, skip + amount)?;
        output.extend_from_slice(&buffer.bytes()[skip..skip + amount]);
        position += amount as u64;
    }
    Ok(output)
}

/// The caller serializes mutation of this name. Partial edge sectors are read
/// independently and preserved. Physical EOF always remains block aligned.
/// The writer still owns synchronization and publication ordering.
fn write_physical(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    offset
        .checked_add(bytes.len() as u64)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let original_length = file.metadata()?.len();
    let mut alignment = Alignment::read(file)?;
    alignment.offset = alignment.offset.max(BLOCK);
    let mut position = offset;
    let mut source = bytes;
    while !source.is_empty() {
        let start = position / alignment.offset as u64 * alignment.offset as u64;
        let skip = usize::try_from(position - start).map_err(|_| io::ErrorKind::InvalidInput)?;
        let amount = source.len().min(QUANTUM - skip);
        let size = (skip + amount).div_ceil(alignment.offset) * alignment.offset;
        let mut buffer = Buffer::new(size, alignment.memory)?;
        // Avoid reading overwritten interior pages. At most two edge sectors
        // require preservation, even for a large append or immutable image.
        if skip != 0 && start < original_length {
            let present = usize::try_from((original_length - start).min(alignment.offset as u64))
                .map_err(|_| io::ErrorKind::InvalidInput)?;
            read_aligned(
                file,
                &mut buffer.bytes_mut()[..alignment.offset],
                start,
                alignment,
                present,
            )?;
        }
        let tail_start = start + size as u64 - alignment.offset as u64;
        if skip + amount < size
            && tail_start < original_length
            && (skip == 0 || size > alignment.offset)
        {
            let present =
                usize::try_from((original_length - tail_start).min(alignment.offset as u64))
                    .map_err(|_| io::ErrorKind::InvalidInput)?;
            read_aligned(
                file,
                &mut buffer.bytes_mut()[size - alignment.offset..],
                tail_start,
                alignment,
                present,
            )?;
        }
        buffer.bytes_mut()[skip..skip + amount].copy_from_slice(&source[..amount]);
        let mut done = 0;
        while done < size {
            let count = match file.write_at(&buffer.bytes()[done..], start + done as u64) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            done += count;
            if done < size
                && (!done.is_multiple_of(alignment.offset)
                    || !done.is_multiple_of(alignment.memory))
            {
                return Err(io::Error::other("unaligned short Direct-I/O write"));
            }
        }
        position += amount as u64;
        source = &source[amount..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FsStorageIo, StorageIo};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fixture {
        root: std::path::PathBuf,
        storage: FsStorageIo,
    }
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "direct-storage-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let storage = FsStorageIo::open(&root).unwrap();
            storage.create_new("object.fdm").unwrap();
            Self { root, storage }
        }
        fn file(&self) -> File {
            open(&self.root.join("object.fdm"), true).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn unaligned_reads_writes_holes_and_truncation_preserve_logical_bytes() {
        let fixture = Fixture::new();
        let io = &fixture.storage;
        let initial: Vec<_> = (0..2 * QUANTUM + 71)
            .map(|n| u8::try_from(n % 251).unwrap())
            .collect();
        io.write_at("object.fdm", 0, &initial).unwrap();
        assert_eq!(
            io.read_exact_at("object.fdm", 3, 4101).unwrap(),
            initial[3..4104]
        );
        let mut expected = initial;
        io.write_at("object.fdm", 4093, &[17; 29]).unwrap();
        expected[4093..4122].fill(17);
        assert_eq!(io.read("object.fdm").unwrap(), expected);
        io.set_len("object.fdm", 73).unwrap();
        io.set_len("object.fdm", 111).unwrap();
        expected.truncate(73);
        expected.resize(111, 0);
        io.write_at("object.fdm", 129, &[3, 9]).unwrap();
        expected.resize(129, 0);
        expected.extend_from_slice(&[3, 9]);
        assert_eq!(io.read("object.fdm").unwrap(), expected);
        assert_eq!(io.object_len("object.fdm").unwrap(), 131);
        assert_eq!(
            io.read_exact_at("object.fdm", 130, 2).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        io.sync_file("object.fdm").unwrap();
        assert_eq!(
            fixture.file().metadata().unwrap().len(),
            PAYLOAD_OFFSET + BLOCK as u64
        );
        io.set_len("object.fdm", 0).unwrap();
        assert!(io.read("object.fdm").unwrap().is_empty());
        assert_eq!(fixture.file().metadata().unwrap().len(), PAYLOAD_OFFSET);
    }

    #[test]
    fn sequential_writer_reuses_known_storage_heads_without_disk_reads() {
        let fixture = Fixture::new();
        let name = ".index.fdx.building";
        fixture.storage.create_new(name).unwrap();
        let before = READ_BYTES.with(std::cell::Cell::get);
        for page in 0..16 {
            fixture
                .storage
                .write_at(name, page * BLOCK as u64, &[71; BLOCK])
                .unwrap();
        }
        fixture.storage.set_len(name, 16 * BLOCK as u64).unwrap();
        fixture.storage.sync_file(name).unwrap();
        assert_eq!(
            READ_BYTES.with(std::cell::Cell::get) - before,
            0,
            "our sequential writer already knows every storage length head"
        );
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        assert_eq!(fixture.storage.read(name).unwrap(), vec![71; 16 * BLOCK]);
    }

    #[test]
    fn independent_reads_obtain_storage_heads_once_per_operation() {
        let fixture = Fixture::new();
        fixture
            .storage
            .write_at("object.fdm", 0, &[37; BLOCK])
            .unwrap();
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        for whole in [false, true] {
            let before = READ_BYTES.with(std::cell::Cell::get);
            let bytes = if whole {
                fixture.storage.read("object.fdm").unwrap()
            } else {
                fixture
                    .storage
                    .read_exact_at("object.fdm", 0, BLOCK)
                    .unwrap()
            };
            assert_eq!(bytes, vec![37; BLOCK]);
            assert_eq!(
                READ_BYTES.with(std::cell::Cell::get) - before,
                3 * BLOCK,
                "one fresh pair of length heads plus one payload block; whole={whole}"
            );
        }
    }

    #[test]
    fn failed_direct_writer_discards_the_preceding_cached_length_head() {
        let fixture = Fixture::new();
        let storage = &fixture.storage;
        storage.write_at("object.fdm", 0, b"old").unwrap();
        let failure = storage.mutate_direct_file("object.fdm", false, |file, previous| {
            write_with_layout(file, previous.unwrap(), 0, b"changed")?;
            Err(io::Error::other(
                "injected error after effective head update",
            ))
        });
        assert!(failure.is_err());
        assert_eq!(storage.object_len("object.fdm").unwrap(), 7);
        storage.write_at("object.fdm", 7, b"-retry").unwrap();
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        assert_eq!(storage.read("object.fdm").unwrap(), b"changed-retry");
    }

    #[test]
    fn interrupted_length_update_keeps_the_preceding_durable_prefix() {
        let fixture = Fixture::new();
        let file = fixture.file();
        write(&file, 0, b"acknowledged").unwrap();
        let previous = layout(&file).unwrap();
        // Crash after payload submission, before publishing its logical end.
        write_physical(&file, PAYLOAD_OFFSET + previous.length, b"uncommitted").unwrap();
        file.sync_data().unwrap();
        assert_eq!(
            read(
                &file,
                0,
                usize::try_from(object_len(&file).unwrap()).unwrap()
            )
            .unwrap(),
            b"acknowledged"
        );
        // A torn alternate head must not discard the preceding durable head.
        let mut torn = header(previous.generation + 1, previous.length + 11);
        torn[24] ^= 1;
        write_physical(&file, (1 - previous.slot) as u64 * BLOCK as u64, &torn).unwrap();
        file.sync_data().unwrap();
        assert_eq!(object_len(&file).unwrap(), previous.length);
        set_head(&file, previous, previous.length + 11).unwrap();
        assert_eq!(read(&file, 0, 23).unwrap(), b"acknowledgeduncommitted");
    }

    #[test]
    fn storage_heads_reject_forks_invalid_lengths_reserved_bytes_and_both_torn() {
        let fixture = Fixture::new();
        let file = fixture.file();
        write(&file, 0, &[7; 70]).unwrap();
        for (first, second) in vec![
            (header(3, 70), header(3, 71)),
            (header(3, BLOCK as u64 + 1), header(4, u64::MAX)),
            (header(0, 0), header(0, 0)),
            ([0; BLOCK], [0; BLOCK]),
        ] {
            write_physical(&file, 0, &first).unwrap();
            write_physical(&file, BLOCK as u64, &second).unwrap();
            assert_eq!(
                layout(&file).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        let mut reserved = header(4, 70);
        reserved[28] = 1;
        reserved[24..28].fill(0);
        let crc = crc32c::crc32c(&reserved);
        reserved[24..28].copy_from_slice(&crc.to_le_bytes());
        write_physical(&file, 0, &reserved).unwrap();
        assert!(layout(&file).is_err());
    }

    #[test]
    fn mutation_revision_invalidates_warm_ranges_even_without_a_timestamp_change() {
        let mut fixture = Fixture::new();
        let counters =
            std::sync::Arc::new(crate::metadata_read_telemetry::MetadataReadCounters::default());
        fixture.storage.metadata_reads = Some(std::sync::Arc::clone(&counters));
        let io = &fixture.storage;
        io.write_at("object.fdm", 0, b"first").unwrap();
        assert_eq!(io.read("object.fdm").unwrap(), b"first");
        let reads = || {
            counters
                .rows()
                .iter()
                .map(|row| row.operations)
                .sum::<u64>()
        };
        let before = reads();
        assert_eq!(io.read("object.fdm").unwrap(), b"first");
        assert_eq!(reads(), before);
        // Enter a cooperating mutation without changing any filesystem byte
        // or timestamp. Its private revision alone must invalidate reuse.
        io.with_file_mutation("object.fdm", || Ok(())).unwrap();
        assert_eq!(io.read("object.fdm").unwrap(), b"first");
        assert_eq!(reads(), before + 1);
        let error: io::Result<()> = io.with_file_mutation("object.fdm", || {
            write(&fixture.file(), 0, b"later")?;
            Err(io::Error::other("injected failure after payload write"))
        });
        assert!(error.is_err());
        assert_eq!(io.read("object.fdm").unwrap(), b"later");
        assert_eq!(reads(), before + 2);
    }

    #[test]
    fn mutable_reduction_heads_always_read_current_storage() {
        let mut fixture = Fixture::new();
        let counters =
            std::sync::Arc::new(crate::metadata_read_telemetry::MetadataReadCounters::default());
        fixture.storage.metadata_reads = Some(std::sync::Arc::clone(&counters));
        let io = &fixture.storage;
        io.create_new("reduction-head.1.fds").unwrap();
        io.write_at("reduction-head.1.fds", 0, b"head").unwrap();
        for _ in 0..2 {
            assert_eq!(io.read("reduction-head.1.fds").unwrap(), b"head");
        }
        assert_eq!(
            counters
                .rows()
                .iter()
                .map(|row| row.operations)
                .sum::<u64>(),
            2
        );
    }

    #[test]
    fn tiny_mutations_leave_no_file_content_in_the_linux_page_cache() {
        let fixture = Fixture::new();
        fixture
            .storage
            .write_at("object.fdm", 0, &[19; 5003])
            .unwrap();
        fixture.storage.set_len("object.fdm", 73).unwrap();
        fixture.storage.sync_file("object.fdm").unwrap();
        fixture.storage.read_exact_at("object.fdm", 5, 67).unwrap();
        // fincore uses mincore on a non-faulting view. It neither reads file
        // content nor evicts pages, so this measures the untouched result.
        let output = std::process::Command::new("fincore")
            .args(["--bytes", "--noheadings", "--output", "RES"])
            .arg(fixture.root.join("object.fdm"))
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "0");
    }
}

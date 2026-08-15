use fastdup_format::{FormatError, RawRecord};

const BLAKE3_ABC: [u8; 32] = [
    0x64, 0x37, 0xb3, 0xac, 0x38, 0x46, 0x51, 0x33, 0xff, 0xb6, 0x3b, 0x75, 0x27, 0x3a, 0x8d, 0xb5,
    0x48, 0xc5, 0x58, 0x46, 0x5d, 0x79, 0xdb, 0x03, 0xfd, 0x35, 0x9c, 0x6c, 0xd5, 0xbd, 0x9d, 0x85,
];

#[test]
fn raw_record_encodes_exact_chunk_identity_and_round_trips() {
    let encoded = RawRecord::encode(b"abc").expect("valid nonempty chunk");

    assert_eq!(&encoded[0..8], b"FDRECD01");
    assert_eq!(&encoded[10..12], &128_u16.to_le_bytes());
    assert_eq!(&encoded[32..36], &256_u32.to_le_bytes());
    assert_eq!(&encoded[36..40], &3_u32.to_le_bytes());
    assert_eq!(&encoded[128..160], &BLAKE3_ABC);
    assert_eq!(&encoded[192..195], b"abc");
    assert!(encoded[195..].iter().all(|byte| *byte == 0));

    let decoded = RawRecord::decode(&encoded).expect("valid encoded record");
    assert_eq!(decoded.chunk_id().bytes(), BLAKE3_ABC);
    assert_eq!(decoded.payload(), b"abc");
}

#[test]
fn raw_record_rejects_stored_payload_corruption() {
    let mut encoded = RawRecord::encode(b"abc").expect("valid nonempty chunk");
    encoded[193] ^= 0x80;

    assert_eq!(
        RawRecord::decode(&encoded),
        Err(FormatError::RecordChecksumMismatch)
    );
}

#[test]
fn raw_record_rejects_payload_with_a_valid_record_crc_but_wrong_chunk_id() {
    let mut encoded = RawRecord::encode(b"abc").expect("valid nonempty chunk");
    encoded[193] ^= 0x80;
    encoded[60..64].fill(0);
    let checksum = crc32c::crc32c(&encoded);
    encoded[60..64].copy_from_slice(&checksum.to_le_bytes());

    assert_eq!(
        RawRecord::decode(&encoded),
        Err(FormatError::ChunkHashMismatch)
    );
}

#[test]
fn raw_record_rejects_a_checksummed_record_too_short_for_its_chunk_table() {
    let mut encoded = RawRecord::encode(b"abc").expect("valid nonempty chunk");
    encoded.truncate(128);
    encoded[32..36].copy_from_slice(&128_u32.to_le_bytes());
    encoded[60..64].fill(0);
    let checksum = crc32c::crc32c(&encoded);
    encoded[60..64].copy_from_slice(&checksum.to_le_bytes());

    assert_eq!(
        RawRecord::decode(&encoded),
        Err(FormatError::InvalidRecordLength(128))
    );
}

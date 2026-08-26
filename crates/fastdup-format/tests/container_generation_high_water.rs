use fastdup_format::{
    CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES, ContainerGenerationHighWaterHash,
    ContainerGenerationHighWaterRecord,
};

#[test]
fn container_generation_high_water_has_stable_bytes_and_hash_chain() {
    let first =
        ContainerGenerationHighWaterRecord::new(1, ContainerGenerationHighWaterHash::ZERO, 1_024)
            .expect("first reservation is valid");
    let first_bytes = first.encode();
    assert_eq!(
        first_bytes.len(),
        CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES
    );
    assert_eq!(&first_bytes[0..8], b"FDCGHW01");
    assert_eq!(&first_bytes[8..10], &1_u16.to_le_bytes());
    assert_eq!(&first_bytes[24..32], &1_u64.to_le_bytes());
    assert_eq!(&first_bytes[64..72], &1_024_u64.to_le_bytes());
    assert_eq!(
        ContainerGenerationHighWaterRecord::decode(&first_bytes),
        Ok(first)
    );

    let second = ContainerGenerationHighWaterRecord::new(
        2,
        ContainerGenerationHighWaterHash::of(&first_bytes),
        2_048,
    )
    .expect("successor reservation is valid");
    let second_bytes = second.encode();
    assert_eq!(
        &second_bytes[32..64],
        &ContainerGenerationHighWaterHash::of(&first_bytes).bytes()
    );
    assert_eq!(
        ContainerGenerationHighWaterRecord::decode(&second_bytes),
        Ok(second)
    );
}

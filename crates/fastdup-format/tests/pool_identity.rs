use fastdup_format::{
    ApplianceId, POOL_IDENTITY_RECORD_BYTES, PoolId, PoolIdentityFormatError, PoolIdentityRecord,
    PoolRole,
};

#[test]
fn pool_identity_has_stable_role_and_appliance_binding_bytes() {
    let appliance_id = ApplianceId::new([0xA1; 16]).expect("Appliance ID is nonzero");
    let pool_id = PoolId::new([0xB2; 16]).expect("Pool ID is nonzero");
    let identity = PoolIdentityRecord::new(appliance_id, pool_id, PoolRole::Metadata);

    let bytes = identity.encode();

    assert_eq!(bytes.len(), POOL_IDENTITY_RECORD_BYTES);
    assert_eq!(&bytes[0..8], b"FDPOOL01");
    assert_eq!(&bytes[8..10], &1_u16.to_le_bytes());
    assert_eq!(&bytes[14..16], &1_u16.to_le_bytes());
    assert_eq!(&bytes[24..40], &[0xA1; 16]);
    assert_eq!(&bytes[40..56], &[0xB2; 16]);
    assert_eq!(PoolIdentityRecord::decode(&bytes), Ok(identity));
    assert_eq!(identity.appliance_id(), appliance_id);
    assert_eq!(identity.pool_id(), pool_id);
    assert_eq!(identity.role(), PoolRole::Metadata);
}

#[test]
fn pool_identity_rejects_corruption_and_unknown_roles() {
    let identity = PoolIdentityRecord::new(
        ApplianceId::new([0x11; 16]).expect("Appliance ID is nonzero"),
        PoolId::new([0x22; 16]).expect("Pool ID is nonzero"),
        PoolRole::Data,
    );
    let mut corrupted = identity.encode();
    corrupted[24] ^= 0x80;
    assert_eq!(
        PoolIdentityRecord::decode(&corrupted),
        Err(PoolIdentityFormatError::ChecksumMismatch)
    );

    let mut unknown_role = identity.encode();
    unknown_role[14..16].copy_from_slice(&9_u16.to_le_bytes());
    unknown_role[56..60].fill(0);
    let checksum = crc32c::crc32c(&unknown_role);
    unknown_role[56..60].copy_from_slice(&checksum.to_le_bytes());
    assert_eq!(
        PoolIdentityRecord::decode(&unknown_role),
        Err(PoolIdentityFormatError::UnsupportedRole(9))
    );
}

use fastdup_appliance::{
    PhysicalPoolIsolation, PoolIsolationError, PoolIsolationObservation, PoolIsolationPolicy,
};

#[test]
fn production_requires_distinct_xfs_devices() {
    let shared = PoolIsolationObservation::new(7, "xfs", 7, "xfs");
    assert_eq!(
        PhysicalPoolIsolation::audit(&shared, PoolIsolationPolicy::Required),
        Err(PoolIsolationError::SharedFilesystem)
    );

    let wrong_format = PoolIsolationObservation::new(7, "ext4", 8, "xfs");
    assert_eq!(
        PhysicalPoolIsolation::audit(&wrong_format, PoolIsolationPolicy::Required),
        Err(PoolIsolationError::UnsupportedFilesystem)
    );

    let isolated = PoolIsolationObservation::new(7, "xfs", 8, "xfs");
    assert_eq!(
        PhysicalPoolIsolation::audit(&isolated, PoolIsolationPolicy::Required),
        Ok(PhysicalPoolIsolation::Enforced)
    );
}

#[test]
fn lab_override_is_explicit_and_never_reports_enforced_isolation() {
    let shared = PoolIsolationObservation::new(7, "overlay", 7, "overlay");
    assert_eq!(
        PhysicalPoolIsolation::audit(&shared, PoolIsolationPolicy::LabAllowShared),
        Ok(PhysicalPoolIsolation::LabBypass)
    );
}

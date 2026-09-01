use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use fastdup_appliance::{
    COMMIT_METADATA_FLOOR_BYTES_V1, CommitCapacityGovernor, CommitCapacitySnapshot,
};
use fastdup_posix::{
    CommitCapacityAdmission, CommitCapacityClaim, CommitToken, CommittedEntry, CommittedFile,
    CommittedInode, CommittedNamespaceSnapshot, InodeAttributesUpdate, InodeId, Namespace,
    NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply, RequestContext,
};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 7,
};

const ROOT_CALLER: RequestContext = RequestContext {
    uid: 0,
    gid: 0,
    pid: 1,
};

#[derive(Debug)]
struct EmptyCommittedFile;

impl CommittedFile for EmptyCommittedFile {
    fn logical_size(&self) -> u64 {
        0
    }

    fn allocated_bytes(&self) -> u64 {
        0
    }

    fn allocated_bytes_in_range(&self, _offset: u64, _length: u64) -> Result<u64, PosixError> {
        Ok(0)
    }

    fn read_at(&self, _offset: u64, _length: u32) -> Result<Vec<u8>, PosixError> {
        Ok(Vec::new())
    }
}

#[derive(Debug)]
struct GatedCreateAccept {
    governor: CommitCapacityGovernor,
    entered: Mutex<Option<Sender<()>>>,
    release: Mutex<Receiver<()>>,
}

#[derive(Debug)]
struct GatedReserve {
    entered: Mutex<Option<Sender<()>>>,
    release: Mutex<Receiver<()>>,
}

impl CommitCapacityAdmission for GatedReserve {
    fn try_reserve(&self, _claim: CommitCapacityClaim) -> Result<(), PosixError> {
        if let Some(entered) = self
            .entered
            .lock()
            .expect("ASSERT: test reserve-entry lock poisoned")
            .take()
        {
            entered.send(()).expect("report delayed reservation");
            self.release
                .lock()
                .expect("ASSERT: test reserve-release lock poisoned")
                .recv()
                .expect("release delayed reservation");
        }
        Ok(())
    }

    fn cancel(&self, _claim: CommitCapacityClaim) {}

    fn accept(&self, _claim: CommitCapacityClaim) {}

    fn release_active_metadata(&self, _bytes: u64) {}

    fn freeze(&self, _token: CommitToken) {}

    fn complete(&self, _token: CommitToken) {}

    fn finish_uncheckpointed_active(&self) {}
}

impl CommitCapacityAdmission for GatedCreateAccept {
    fn try_reserve(&self, claim: CommitCapacityClaim) -> Result<(), PosixError> {
        CommitCapacityAdmission::try_reserve(&self.governor, claim)
    }

    fn cancel(&self, claim: CommitCapacityClaim) {
        CommitCapacityAdmission::cancel(&self.governor, claim);
    }

    fn accept(&self, claim: CommitCapacityClaim) {
        if let Some(entered) = self
            .entered
            .lock()
            .expect("ASSERT: test accept-entry lock poisoned")
            .take()
        {
            entered.send(()).expect("report pre-accept window");
            self.release
                .lock()
                .expect("ASSERT: test accept-release lock poisoned")
                .recv()
                .expect("release delayed accept");
        }
        CommitCapacityAdmission::accept(&self.governor, claim);
    }

    fn release_active_metadata(&self, bytes: u64) {
        CommitCapacityAdmission::release_active_metadata(&self.governor, bytes);
    }

    fn freeze(&self, token: CommitToken) {
        CommitCapacityAdmission::freeze(&self.governor, token);
    }

    fn complete(&self, token: CommitToken) {
        CommitCapacityAdmission::complete(&self.governor, token);
    }

    fn finish_uncheckpointed_active(&self) {
        CommitCapacityAdmission::finish_uncheckpointed_active(&self.governor);
    }
}

#[test]
fn rejected_write_never_enters_the_live_namespace() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 4 * 1_024 * 1_024,
            8 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor);

    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"capacity",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("metadata mutation fits")
    else {
        panic!("create reply")
    };

    let error = namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: &[0x5a; 8 * 1_024],
            },
        )
        .expect_err("pessimistic DATA claim exceeds capacity");
    assert_eq!(error, PosixError::NoSpace);

    let Reply::Data(bytes) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: 16 * 1_024,
            },
        )
        .expect("reads remain available")
    else {
        panic!("read reply")
    };
    assert!(bytes.is_empty());

    namespace
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"capacity",
            },
        )
        .expect("capacity-reducing cleanup remains available");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"capacity",
            },
        ),
        Err(PosixError::NoEntry)
    );
}

#[test]
fn policy_selected_small_file_reserves_metadata_and_its_own_quota_not_data() {
    const SMALL_WRITE_CLAIM: u64 = 2 * 256 * 1_024 + 4 * 1_024;
    let governor = Arc::new(
        CommitCapacityGovernor::new(
            CommitCapacitySnapshot::new(COMMIT_METADATA_FLOOR_BYTES_V1 + 32 * 1_024 * 1_024, 0)
                .with_small_file_available_bytes(SMALL_WRITE_CLAIM),
        )
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"policy.json",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("commit-critical create fits without DATA")
    else {
        panic!("create reply")
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: &[0x5a; 1_024],
            },
        )
        .expect("Small-File quota admits the first write");
    let status = governor.status();
    assert_eq!(status.reserved_data_bytes(), 0);
    assert_eq!(status.active_data_bytes(), 0);
    assert_eq!(status.reserved_small_file_bytes(), SMALL_WRITE_CLAIM);
    assert_eq!(status.active_small_file_bytes(), SMALL_WRITE_CLAIM);

    let error = namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 1_024,
                data: &[0x6b; 1_024],
            },
        )
        .expect_err("the independent Small-File bucket is exhausted");
    assert_eq!(error, PosixError::NoSpace);
}

#[test]
fn uncheckpointed_small_file_physical_claim_stays_charged_to_metadata_until_observed() {
    let physical = 512 * 1_024;
    let structural = 2 * 1_024 * 1_024;
    let snapshot =
        CommitCapacitySnapshot::new(COMMIT_METADATA_FLOOR_BYTES_V1 + 8 * 1_024 * 1_024, 0)
            .with_small_file_available_bytes(physical);
    let governor = CommitCapacityGovernor::new(snapshot).expect("capacity floor fits");
    let claim = CommitCapacityClaim::with_small_file_bytes(structural + physical, physical);
    governor.try_reserve(claim).expect("claim fits both tiers");
    governor.accept(claim);
    governor.finish_uncheckpointed_active();

    let pending = governor.status();
    assert_eq!(pending.active_metadata_bytes(), 0);
    assert_eq!(pending.active_small_file_bytes(), 0);
    assert_eq!(
        pending.reserved_metadata_bytes(),
        COMMIT_METADATA_FLOOR_BYTES_V1 + physical
    );
    assert_eq!(pending.reserved_small_file_bytes(), physical);

    let observation = governor.begin_observation();
    governor.finish_observation(observation, snapshot);
    let observed = governor.status();
    assert_eq!(
        observed.reserved_metadata_bytes(),
        COMMIT_METADATA_FLOOR_BYTES_V1
    );
    assert_eq!(observed.reserved_small_file_bytes(), 0);
}

#[test]
fn completed_commit_claim_waits_for_a_new_capacity_observation() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());

    let Reply::Entry(_) = namespace
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"generation",
                mode: 0o700,
            },
        )
        .expect("mkdir")
    else {
        panic!("mkdir reply")
    };
    let before_cut = governor.status();
    assert!(before_cut.active_metadata_bytes() > 0);

    let commit = namespace
        .begin_commit()
        .expect("cut")
        .expect("dirty generation");
    let frozen = governor.status();
    assert_eq!(frozen.active_metadata_bytes(), 0);
    assert_eq!(frozen.frozen_generations(), 1);

    let failed_attempt_observation = governor.begin_observation();
    governor.finish_observation(
        failed_attempt_observation,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ),
    );
    assert_eq!(
        governor.status().frozen_generations(),
        1,
        "a failed or uncompleted publication retains its capacity"
    );

    namespace
        .complete_commit(&commit, Vec::new())
        .expect("metadata-only commit completes");
    assert_eq!(governor.status().frozen_generations(), 1);

    let observation = governor.begin_observation();
    governor.finish_observation(
        observation,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ),
    );
    let observed = governor.status();
    assert_eq!(observed.frozen_generations(), 0);
    assert_eq!(
        observed.reserved_metadata_bytes(),
        COMMIT_METADATA_FLOOR_BYTES_V1
    );
}

#[test]
fn observation_started_before_commit_cannot_release_that_commit() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    namespace
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"sample-race",
                mode: 0o700,
            },
        )
        .expect("mkdir");
    let commit = namespace
        .begin_commit()
        .expect("cut")
        .expect("dirty generation");

    let stale_observation = governor.begin_observation();
    namespace
        .complete_commit(&commit, Vec::new())
        .expect("commit completes while sample is running");
    governor.finish_observation(
        stale_observation,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ),
    );
    assert_eq!(governor.status().frozen_generations(), 1);

    let fresh_observation = governor.begin_observation();
    governor.finish_observation(
        fresh_observation,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ),
    );
    assert_eq!(governor.status().frozen_generations(), 0);
}

#[test]
#[allow(clippy::too_many_lines)]
fn open_orphan_data_claim_waits_for_a_fresh_capacity_observation() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 4 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"committed-open-orphan",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create fixture")
    else {
        panic!("create reply")
    };
    let inode = entry.attr.inode;

    let create = namespace
        .begin_commit()
        .expect("cut create")
        .expect("create is dirty");
    namespace
        .complete_commit(
            &create,
            vec![fastdup_posix::CommittedFileInstall::new(
                inode,
                0,
                Arc::new(EmptyCommittedFile),
            )],
        )
        .expect("complete create");
    let after_create = governor.begin_observation();
    governor.finish_observation(
        after_create,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 4 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ),
    );

    namespace
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"committed-open-orphan",
            },
        )
        .expect("unlink while the handle remains open");
    let unlink = namespace
        .begin_commit()
        .expect("cut unlink")
        .expect("unlink is dirty");
    namespace
        .complete_commit(&unlink, Vec::new())
        .expect("complete unlink");
    let after_unlink = governor.begin_observation();
    governor.finish_observation(
        after_unlink,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 4 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ),
    );
    assert_eq!(governor.status().reserved_data_bytes(), 0);

    let stale_observation = governor.begin_observation();
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"irreversible orphan payload",
            },
        )
        .expect("the open orphan remains writable");
    let accepted_data = governor.status().reserved_data_bytes();
    assert!(accepted_data > 0, "the write reserves physical DATA");
    assert!(
        namespace
            .begin_commit()
            .expect("inspect checkpointability")
            .is_none(),
        "an open orphan has no recoverable Namespace root"
    );
    let uncheckpointed = governor.status();
    assert_eq!(uncheckpointed.active_metadata_bytes(), 0);
    assert_eq!(uncheckpointed.active_data_bytes(), 0);
    assert_eq!(
        uncheckpointed.reserved_data_bytes(),
        accepted_data,
        "write-through DATA remains charged even though no Commit can name the orphan"
    );

    governor.finish_observation(
        stale_observation,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 4 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ),
    );
    assert_eq!(
        governor.status().reserved_data_bytes(),
        accepted_data,
        "an observation started before the write cannot account for its DATA"
    );

    let fresh_observation = governor.begin_observation();
    governor.finish_observation(
        fresh_observation,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 4 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ),
    );
    assert_eq!(
        governor.status().reserved_data_bytes(),
        0,
        "a later successful physical observation may retire the orphan DATA claim"
    );
}

#[test]
fn sequential_128k_writes_do_not_exhaust_metadata_by_syscall_count() {
    const WRITE_BYTES: usize = 128 * 1_024;
    const WRITE_COUNT: usize = 128;

    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 16 * 1_024 * 1_024,
            128 * 1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"sequential-small-writes",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create sequential fixture")
    else {
        panic!("create reply")
    };
    let payload = vec![0x5a; WRITE_BYTES];

    for ordinal in 0..WRITE_COUNT {
        namespace
            .dispatch(
                CALLER,
                Operation::Write {
                    inode: entry.attr.inode,
                    handle,
                    offset: u64::try_from(ordinal * WRITE_BYTES).expect("fixture offset fits"),
                    data: &payload,
                },
            )
            .expect("sequential write must not reserve one Manifest path per syscall");
    }

    let status = governor.status();
    assert_eq!(
        status.active_metadata_bytes(),
        10 * 1_024 * 1_024,
        "create plus one 16-MiB sequential extent has an exact bounded claim"
    );
    let Reply::Attr(attributes) = namespace
        .dispatch(
            CALLER,
            Operation::GetAttr {
                inode: entry.attr.inode,
            },
        )
        .expect("read final size")
    else {
        panic!("attribute reply")
    };
    assert_eq!(
        attributes.size,
        u64::try_from(WRITE_BYTES * WRITE_COUNT).unwrap()
    );
}

#[test]
fn discontinuous_writes_retain_one_manifest_path_claim_each() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 6 * 1_024 * 1_024,
            8 * 1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor);
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"discontinuous-writes",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create discontinuous fixture")
    else {
        panic!("create reply")
    };

    for offset in [1, 3] {
        namespace
            .dispatch(
                CALLER,
                Operation::Write {
                    inode: entry.attr.inode,
                    handle,
                    offset,
                    data: b"x",
                },
            )
            .expect("two discontinuous Manifest paths fit");
    }
    let error = namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 5,
                data: b"x",
            },
        )
        .expect_err("a third discontinuous Manifest path exceeds Metadata headroom");
    assert_eq!(error, PosixError::NoSpace);
}

#[test]
fn one_byte_file_claims_one_manifest_path_not_a_full_large_append_window() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024 + 40 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"one-byte",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("one inode and directory entry fit one Metadata path claim")
    else {
        panic!("create reply")
    };

    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"x",
            },
        )
        .expect("one tiny Manifest fits one additional Metadata path claim");

    assert_eq!(
        governor.status().active_metadata_bytes(),
        2 * 1_024 * 1_024 + 40 * 1_024,
        "create plus one-leaf tiny Manifest have their exact bounded claims"
    );
}

#[test]
fn sparse_growth_requires_metadata_capacity_but_shrink_remains_available() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"sparse-growth",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("prepare fixture before installing constrained admission")
    else {
        panic!("create reply")
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"seed",
            },
        )
        .expect("prepare nonempty fixture");
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    namespace.install_commit_capacity_admission(governor);

    let error = namespace
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: Some(handle),
                length: 8 * 1_024 * 1_024,
            },
        )
        .expect_err("sparse growth needs one Manifest path claim");
    assert_eq!(error, PosixError::NoSpace);
    let Reply::Attr(attributes) = namespace
        .dispatch(
            CALLER,
            Operation::GetAttr {
                inode: entry.attr.inode,
            },
        )
        .expect("failed growth leaves attributes readable")
    else {
        panic!("attribute reply")
    };
    assert_eq!(attributes.size, 4);

    namespace
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: Some(handle),
                length: 0,
            },
        )
        .expect("capacity-reducing shrink remains available at the floor");
}

#[test]
fn successful_metadata_noops_need_no_commit_capacity() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"noop",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("prepare fixture before installing constrained admission")
    else {
        panic!("create reply")
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Release {
                inode: entry.attr.inode,
                handle,
            },
        )
        .expect("release fixture handle");
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    namespace.install_commit_capacity_admission(governor.clone());

    let Reply::Created { handle, .. } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"noop",
                mode: 0o600,
                options: OpenOptions::READ_ONLY,
                exclusive: false,
                truncate: false,
            },
        )
        .expect("O_CREAT on an existing inode is only an open")
    else {
        panic!("create-existing reply")
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Release {
                inode: entry.attr.inode,
                handle,
            },
        )
        .expect("release reopened handle");
    namespace
        .dispatch(
            CALLER,
            Operation::SetAttributes {
                inode: entry.attr.inode,
                update: InodeAttributesUpdate::default(),
            },
        )
        .expect("an empty setattr is getattr-equivalent");
    namespace
        .dispatch(
            CALLER,
            Operation::Rename {
                parent: ROOT_INODE,
                name: b"noop",
                new_parent: ROOT_INODE,
                new_name: b"noop",
                no_replace: false,
            },
        )
        .expect("rename onto the same name is a no-op");
    namespace
        .dispatch(
            ROOT_CALLER,
            Operation::SetFileFlags {
                inode: entry.attr.inode,
                flags: 0,
            },
        )
        .expect("installing unchanged file flags is a no-op");

    assert_eq!(governor.status().active_metadata_bytes(), 0);
}

#[test]
fn repeated_zero_length_clones_do_not_consume_commit_capacity() {
    let snapshot = CommittedNamespaceSnapshot::new(
        4,
        4_096,
        7,
        vec![
            CommittedInode::new(2, 0o600, 1_000, 1_000, 1, 7, Arc::new(EmptyCommittedFile))
                .expect("committed clone source"),
            CommittedInode::new(3, 0o600, 1_000, 1_000, 1, 7, Arc::new(EmptyCommittedFile))
                .expect("committed clone target"),
        ],
        vec![
            CommittedEntry::new(1, 2, b"clone-source".to_vec()).expect("source entry"),
            CommittedEntry::new(1, 3, b"clone-target".to_vec()).expect("target entry"),
        ],
    )
    .expect("committed clone fixture");
    let namespace = Namespace::from_committed_writable(NamespaceConfig::default(), snapshot)
        .expect("mount committed clone fixture writable");
    let source = InodeId::new(2).expect("source inode");
    let target = InodeId::new(3).expect("target inode");
    let Reply::Opened(source_handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: source,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        )
        .expect("open clone source")
    else {
        panic!("open source reply")
    };
    let Reply::Opened(target_handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: target,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        )
        .expect("open clone target")
    else {
        panic!("open target reply")
    };

    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    namespace.install_commit_capacity_admission(governor.clone());

    for _ in 0..16 {
        let Reply::Cloned { bytes, .. } = namespace
            .dispatch(
                CALLER,
                Operation::CloneRange {
                    source_inode: source,
                    source_handle,
                    source_offset: 0,
                    target_inode: target,
                    target_handle,
                    target_offset: 0,
                    length: 0,
                },
            )
            .expect("a zero-length clone is a successful no-op")
        else {
            panic!("clone reply")
        };
        assert_eq!(bytes, 0);
    }

    let status = governor.status();
    assert_eq!(status.active_metadata_bytes(), 0);
    assert_eq!(
        status.reserved_metadata_bytes(),
        COMMIT_METADATA_FLOOR_BYTES_V1
    );
    assert!(
        namespace
            .begin_commit()
            .expect("zero-length clones leave commit coordination healthy")
            .is_none(),
        "zero-length clones must not create a Dirty Commit"
    );
}

#[test]
fn create_then_unlink_in_one_active_epoch_releases_the_create_claim() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());

    for ordinal in 0..2 {
        let name = format!("temporary-{ordinal}");
        let Reply::Created { entry, handle } = namespace
            .dispatch(
                CALLER,
                Operation::Create {
                    parent: ROOT_INODE,
                    name: name.as_bytes(),
                    mode: 0o600,
                    options: OpenOptions::READ_WRITE,
                    exclusive: true,
                    truncate: false,
                },
            )
            .expect("a prior net-zero create/unlink must not exhaust Metadata")
        else {
            panic!("create reply")
        };
        namespace
            .dispatch(
                CALLER,
                Operation::Unlink {
                    parent: ROOT_INODE,
                    name: name.as_bytes(),
                },
            )
            .expect("unlink remains available");
        namespace
            .dispatch(
                CALLER,
                Operation::Release {
                    inode: entry.attr.inode,
                    handle,
                },
            )
            .expect("release open orphan");
        assert_eq!(
            governor.status().active_metadata_bytes(),
            0,
            "the active Namespace returned to its pre-create representation"
        );
    }
}

#[test]
fn directory_create_claim_is_published_atomically_with_the_new_inode() {
    let (accept_entered_tx, accept_entered_rx) = mpsc::channel();
    let (accept_release_tx, accept_release_rx) = mpsc::channel();
    let admission = Arc::new(GatedCreateAccept {
        governor: CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
        entered: Mutex::new(Some(accept_entered_tx)),
        release: Mutex::new(accept_release_rx),
    });
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    namespace.install_commit_capacity_admission(admission.clone());

    let create_namespace = namespace.clone();
    let create = thread::spawn(move || {
        create_namespace.dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"atomic-create-claim",
                mode: 0o700,
            },
        )
    });
    accept_entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("mkdir reaches its delayed capacity acceptance");

    let remove_namespace = namespace.clone();
    let remove = thread::spawn(move || {
        panic::catch_unwind(AssertUnwindSafe(|| {
            remove_namespace.dispatch(
                CALLER,
                Operation::Rmdir {
                    parent: ROOT_INODE,
                    name: b"atomic-create-claim",
                },
            )
        }))
    });
    thread::sleep(Duration::from_millis(100));
    accept_release_tx
        .send(())
        .expect("release delayed capacity acceptance");

    create
        .join()
        .expect("mkdir thread remains healthy")
        .expect("mkdir succeeds");
    let removal = remove.join().expect("rmdir thread remains joinable");
    let removal = removal
        .expect("rmdir must not underflow capacity while mkdir publishes its claim attribution");
    removal.expect("rmdir succeeds after the create claim becomes active");

    let status = admission.governor.status();
    assert_eq!(status.active_metadata_bytes(), 0);
    assert_eq!(
        status.reserved_metadata_bytes(),
        COMMIT_METADATA_FLOOR_BYTES_V1,
        "the removed directory leaves no stranded create claim"
    );
}

#[test]
fn durable_dispatch_does_not_reacquire_mutation_fence_behind_commit_cut() {
    let (reserve_entered_tx, reserve_entered_rx) = mpsc::channel();
    let (reserve_release_tx, reserve_release_rx) = mpsc::channel();
    let admission = Arc::new(GatedReserve {
        entered: Mutex::new(Some(reserve_entered_tx)),
        release: Mutex::new(reserve_release_rx),
    });
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let Reply::Entry(fixture) = namespace
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"fence-fixture",
                mode: 0o700,
            },
        )
        .expect("create non-root fence fixture")
    else {
        panic!("mkdir reply")
    };
    namespace.install_commit_capacity_admission(admission);

    let (dispatch_done_tx, dispatch_done_rx) = mpsc::channel();
    let dispatch_namespace = namespace.clone();
    let dispatch = thread::spawn(move || {
        let result = dispatch_namespace.dispatch(
            ROOT_CALLER,
            Operation::SetMode {
                inode: fixture.attr.inode,
                mode: 0o755,
            },
        );
        dispatch_done_tx
            .send(result)
            .expect("report durable dispatch completion");
    });
    reserve_entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("dispatch reaches reservation while holding its mutation fence");

    let (commit_started_tx, commit_started_rx) = mpsc::channel();
    let commit_namespace = namespace.clone();
    let commit = thread::spawn(move || {
        commit_started_tx.send(()).expect("report Commit start");
        commit_namespace.begin_commit()
    });
    commit_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("Commit thread starts");
    thread::sleep(Duration::from_millis(100));
    reserve_release_tx
        .send(())
        .expect("release delayed reservation");

    dispatch_done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("durable dispatch must not deadlock behind the waiting Commit writer")
        .expect("chmod succeeds");
    dispatch.join().expect("dispatch thread remains healthy");
    assert!(
        commit
            .join()
            .expect("Commit thread remains healthy")
            .expect("Commit cut succeeds")
            .is_some(),
        "the admitted chmod enters the Commit cut"
    );
}

#[test]
fn temporary_directories_and_symlinks_release_their_active_create_claims() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());

    namespace
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"temporary-directory",
                mode: 0o700,
            },
        )
        .expect("temporary directory fits");
    namespace
        .dispatch(
            CALLER,
            Operation::Rmdir {
                parent: ROOT_INODE,
                name: b"temporary-directory",
            },
        )
        .expect("remove temporary directory");
    assert_eq!(governor.status().active_metadata_bytes(), 0);

    namespace
        .dispatch(
            CALLER,
            Operation::Symlink {
                parent: ROOT_INODE,
                name: b"temporary-symlink",
                target: b"missing-target",
            },
        )
        .expect("temporary symlink reuses the same headroom");
    namespace
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"temporary-symlink",
            },
        )
        .expect("remove temporary symlink");
    assert_eq!(governor.status().active_metadata_bytes(), 0);
}

#[test]
fn rename_over_an_active_new_inode_releases_the_replaced_create_claim() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 6 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());

    for name in [b"source".as_slice(), b"target".as_slice()] {
        namespace
            .dispatch(
                CALLER,
                Operation::Create {
                    parent: ROOT_INODE,
                    name,
                    mode: 0o600,
                    options: OpenOptions::READ_WRITE,
                    exclusive: true,
                    truncate: false,
                },
            )
            .expect("create rename fixture");
    }
    namespace
        .dispatch(
            CALLER,
            Operation::Rename {
                parent: ROOT_INODE,
                name: b"source",
                new_parent: ROOT_INODE,
                new_name: b"target",
                no_replace: false,
            },
        )
        .expect("replace active target");

    assert_eq!(
        governor.status().active_metadata_bytes(),
        4 * 1_024 * 1_024,
        "source create plus rename remain; replaced create is net-zero"
    );
}

#[test]
fn unlink_after_a_commit_cut_cannot_release_the_frozen_create_claim() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"frozen-create",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create fixture");

    namespace
        .begin_commit()
        .expect("form commit cut")
        .expect("dirty generation");
    namespace
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"frozen-create",
            },
        )
        .expect("unlink enters the next Active epoch");

    let status = governor.status();
    assert_eq!(status.active_metadata_bytes(), 0);
    assert_eq!(status.frozen_generations(), 1);
    assert_eq!(
        status.reserved_metadata_bytes(),
        COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
        "the Frozen Commit can still publish the file"
    );
}

#[test]
fn directory_rename_replacement_releases_the_active_target_create_claim() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 6 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    for name in [
        b"source-directory".as_slice(),
        b"target-directory".as_slice(),
    ] {
        namespace
            .dispatch(
                CALLER,
                Operation::Mkdir {
                    parent: ROOT_INODE,
                    name,
                    mode: 0o700,
                },
            )
            .expect("create directory fixture");
    }
    namespace
        .dispatch(
            CALLER,
            Operation::Rename {
                parent: ROOT_INODE,
                name: b"source-directory",
                new_parent: ROOT_INODE,
                new_name: b"target-directory",
                no_replace: false,
            },
        )
        .expect("replace empty active directory");
    assert_eq!(governor.status().active_metadata_bytes(), 4 * 1_024 * 1_024);
}

#[test]
fn inode_created_before_capacity_admission_has_no_releasable_claim() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"predates-governor",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create fixture before installing capacity admission");
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    namespace.install_commit_capacity_admission(governor.clone());

    namespace
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"predates-governor",
            },
        )
        .expect("cleanup cannot release a claim that was never reserved");
    assert_eq!(governor.status().active_metadata_bytes(), 0);
}

#[test]
fn relatime_update_claims_metadata_once_without_blocking_reads() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"relatime",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("prepare fixture before installing constrained admission")
    else {
        panic!("create reply")
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"readable",
            },
        )
        .expect("prepare readable fixture");
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 2 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    namespace.install_commit_capacity_admission(governor.clone());
    let Reply::Attr(before_read) = namespace
        .dispatch(
            CALLER,
            Operation::GetAttr {
                inode: entry.attr.inode,
            },
        )
        .expect("read initial mutation sequence")
    else {
        panic!("attribute reply")
    };

    for _ in 0..2 {
        let Reply::Data(bytes) = namespace
            .dispatch(
                CALLER,
                Operation::Read {
                    inode: entry.attr.inode,
                    handle,
                    offset: 0,
                    length: 8,
                },
            )
            .expect("reads remain available while relatime is conditional")
        else {
            panic!("read reply")
        };
        assert_eq!(bytes, b"readable");
    }
    assert_eq!(
        governor.status().active_metadata_bytes(),
        2 * 1_024 * 1_024,
        "the first read updates relatime; the second is a Metadata no-op"
    );
    let Reply::Attr(after_read) = namespace
        .dispatch(
            CALLER,
            Operation::GetAttr {
                inode: entry.attr.inode,
            },
        )
        .expect("read final mutation sequence")
    else {
        panic!("attribute reply")
    };
    assert_eq!(
        after_read.mutation_sequence,
        before_read.mutation_sequence + 1,
        "one persisted relatime change is one inode version"
    );
}

#[test]
fn exhausted_relatime_capacity_never_blocks_the_read() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"relatime-exhausted",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("prepare fixture before installing constrained admission")
    else {
        panic!("create reply")
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"readable",
            },
        )
        .expect("prepare readable fixture");
    let Reply::Attr(before_read) = namespace
        .dispatch(
            CALLER,
            Operation::GetAttr {
                inode: entry.attr.inode,
            },
        )
        .expect("read initial mutation sequence")
    else {
        panic!("attribute reply")
    };
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    namespace.install_commit_capacity_admission(governor.clone());

    let Reply::Data(bytes) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: 8,
            },
        )
        .expect("data read is independent of optional relatime capacity")
    else {
        panic!("read reply")
    };
    assert_eq!(bytes, b"readable");
    let Reply::Attr(after_read) = namespace
        .dispatch(
            CALLER,
            Operation::GetAttr {
                inode: entry.attr.inode,
            },
        )
        .expect("read final mutation sequence")
    else {
        panic!("attribute reply")
    };
    assert_eq!(after_read.mutation_sequence, before_read.mutation_sequence);
    assert_eq!(governor.status().active_metadata_bytes(), 0);
}

#[test]
fn failed_metadata_storm_cancels_every_temporary_claim() {
    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + 4 * 1_024 * 1_024,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    namespace
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"taken",
                mode: 0o700,
            },
        )
        .expect("first directory consumes one path claim");

    for _ in 0..1_024 {
        assert_eq!(
            namespace.dispatch(
                CALLER,
                Operation::Mkdir {
                    parent: ROOT_INODE,
                    name: b"taken",
                    mode: 0o700,
                },
            ),
            Err(PosixError::Exists)
        );
    }
    namespace
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"later",
                mode: 0o700,
            },
        )
        .expect("all failed-operation claims were cancelled");
    assert_eq!(governor.status().active_metadata_bytes(), 4 * 1_024 * 1_024);
}

#[test]
fn excessive_metadata_batch_releases_after_commit_and_fresh_observation() {
    const DIRECTORY_COUNT: usize = 256;
    const METADATA_CLAIM_BYTES: u64 = 2 * 1_024 * 1_024;
    const OPERATION_COUNT: u64 = 2 * DIRECTORY_COUNT as u64;
    const BATCH_CLAIM_BYTES: u64 = OPERATION_COUNT * METADATA_CLAIM_BYTES;

    let governor = Arc::new(
        CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + BATCH_CLAIM_BYTES,
            1_024 * 1_024,
        ))
        .expect("capacity floor fits"),
    );
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    namespace.install_commit_capacity_admission(governor.clone());
    let mut inodes = Vec::with_capacity(DIRECTORY_COUNT);

    for ordinal in 0..DIRECTORY_COUNT {
        let name = format!("metadata-{ordinal:04}");
        let Reply::Entry(entry) = namespace
            .dispatch(
                CALLER,
                Operation::Mkdir {
                    parent: ROOT_INODE,
                    name: name.as_bytes(),
                    mode: 0o700,
                },
            )
            .expect("Metadata batch create fits its exact claim budget")
        else {
            panic!("mkdir reply")
        };
        inodes.push(entry.attr.inode);
    }
    for inode in inodes {
        namespace
            .dispatch(CALLER, Operation::SetMode { inode, mode: 0o750 })
            .expect("Metadata batch chmod fits its exact claim budget");
    }
    assert_eq!(governor.status().active_metadata_bytes(), BATCH_CLAIM_BYTES);

    let commit = namespace
        .begin_commit()
        .expect("cut Metadata stress generation")
        .expect("Metadata stress generation is dirty");
    namespace
        .complete_commit(&commit, Vec::new())
        .expect("complete Metadata-only stress generation");
    let observation = governor.begin_observation();
    governor.finish_observation(
        observation,
        CommitCapacitySnapshot::new(
            COMMIT_METADATA_FLOOR_BYTES_V1 + BATCH_CLAIM_BYTES,
            1_024 * 1_024,
        ),
    );
    assert_eq!(
        governor.status().reserved_metadata_bytes(),
        COMMIT_METADATA_FLOOR_BYTES_V1,
        "the fresh physical observation retires the complete stress batch"
    );
}

use fastdup_posix::{
    AccessMode, Namespace, NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply,
    RequestContext,
};
use std::collections::BTreeSet;
use std::sync::Arc;

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 42,
};

#[test]
#[allow(clippy::too_many_lines)]
fn acknowledged_writes_are_live_and_open_orphans_remain_usable() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let raw_name = b"vm-\xff";

    let Reply::Created {
        entry,
        handle: writer,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: raw_name,
                mode: 0o640,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create must succeed")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };

    let inode = entry.attr.inode;
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: raw_name,
            },
        ),
        Ok(Reply::Entry(entry.clone()))
    );

    let Reply::Opened(reader) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        )
        .expect("open must succeed")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: 0,
                data: b"abcdef",
            },
        ),
        Ok(Reply::Written {
            bytes: 6,
            mutation_sequence: 1,
        })
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: reader,
                offset: 2,
                data: b"ZZ",
            },
        ),
        Ok(Reply::Written {
            bytes: 2,
            mutation_sequence: 2,
        })
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle: reader,
                offset: 0,
                length: 6,
            },
        ),
        Ok(Reply::Data(b"abZZef".to_vec()))
    );

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: raw_name,
            },
        ),
        Ok(Reply::Empty)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: raw_name,
            },
        ),
        Err(PosixError::NoEntry)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle: writer,
                offset: 0,
                length: 6,
            },
        ),
        Ok(Reply::Data(b"abZZef".to_vec()))
    );

    for handle in [writer, reader] {
        assert_eq!(
            namespace.dispatch(CALLER, Operation::Release { inode, handle }),
            Ok(Reply::Empty)
        );
    }
    let Reply::Attr(orphan_attr) = namespace
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("lookup references must pin the unlinked inode")
    else {
        panic!("ASSERT: getattr returned the wrong reply variant");
    };
    assert_eq!(orphan_attr.link_count, 0);
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Forget {
                inode,
                lookup_count: 2,
            },
        ),
        Ok(Reply::Empty)
    );
    assert_eq!(
        namespace.dispatch(CALLER, Operation::GetAttr { inode }),
        Err(PosixError::NoEntry)
    );

    let Reply::Created {
        entry: recreated, ..
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: raw_name,
                mode: 0o600,
                options: OpenOptions::READ_ONLY,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("recreate must succeed")
    else {
        panic!("ASSERT: recreate returned the wrong reply variant");
    };
    assert!(recreated.attr.inode > inode, "inode IDs must not be reused");
}

#[test]
#[allow(clippy::too_many_lines)]
fn atomic_rename_replaces_the_visible_name_but_keeps_the_old_open_inode() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created {
        entry: staging,
        handle: staging_handle,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"staging.vbk",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create staging full")
    else {
        panic!("ASSERT: staging create reply");
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: staging.attr.inode,
                handle: staging_handle,
                offset: 0,
                data: b"synthetic-full",
            },
        )
        .expect("write staging full");
    let Reply::Created {
        entry: old,
        handle: old_handle,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"active.vbk",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create old full")
    else {
        panic!("ASSERT: old create reply");
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: old.attr.inode,
                handle: old_handle,
                offset: 0,
                data: b"old-full",
            },
        )
        .expect("write old full");

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Rename {
                parent: ROOT_INODE,
                name: b"staging.vbk",
                new_parent: ROOT_INODE,
                new_name: b"active.vbk",
                no_replace: true,
            },
        ),
        Err(PosixError::Exists)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Rename {
                parent: ROOT_INODE,
                name: b"staging.vbk",
                new_parent: ROOT_INODE,
                new_name: b"active.vbk",
                no_replace: false,
            },
        ),
        Ok(Reply::Empty)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"staging.vbk",
            },
        ),
        Err(PosixError::NoEntry)
    );
    let Reply::Entry(active) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"active.vbk",
            },
        )
        .expect("renamed full is visible")
    else {
        panic!("ASSERT: active lookup reply");
    };
    assert_eq!(active.attr.inode, staging.attr.inode);
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode: old.attr.inode,
                handle: old_handle,
                offset: 0,
                length: 32,
            },
        ),
        Ok(Reply::Data(b"old-full".to_vec())),
        "the replaced inode remains readable through its existing handle"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn seek_write_truncate_and_access_modes_are_checked() {
    let namespace = Namespace::new_volatile(NamespaceConfig {
        maximum_name_bytes: 255,
        maximum_file_bytes: 16 * 1_024,
    });
    let Reply::Created {
        entry,
        handle: writer,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"disk.raw",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create must succeed")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;

    let Reply::Opened(read_only) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("read-only open must succeed")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    let Reply::Opened(write_only) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions {
                    access: AccessMode::WriteOnly,
                    append: false,
                },
                truncate: false,
            },
        )
        .expect("write-only open must succeed")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: 8_192,
                data: b"X",
            },
        ),
        Ok(Reply::Written {
            bytes: 1,
            mutation_sequence: 1,
        })
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle: read_only,
                offset: 8_188,
                length: 8,
            },
        ),
        Ok(Reply::Data(vec![0, 0, 0, 0, b'X']))
    );

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: read_only,
                offset: 0,
                data: b"denied",
            },
        ),
        Err(PosixError::BadHandle)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle: write_only,
                offset: 0,
                length: 1,
            },
        ),
        Err(PosixError::BadHandle)
    );

    let Reply::Attr(shrunk) = namespace
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode,
                handle: Some(writer),
                length: 4,
            },
        )
        .expect("truncate must succeed")
    else {
        panic!("ASSERT: truncate returned the wrong reply variant");
    };
    assert_eq!(shrunk.size, 4);
    assert_eq!(shrunk.mutation_sequence, 2);

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: 10_000,
                data: b"",
            },
        ),
        Ok(Reply::Written {
            bytes: 0,
            mutation_sequence: 2,
        })
    );
    assert_eq!(
        namespace.dispatch(CALLER, Operation::GetAttr { inode }),
        Ok(Reply::Attr(shrunk))
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: 16 * 1_024,
                data: b"too large",
            },
        ),
        Err(PosixError::FileTooLarge)
    );
}

#[test]
fn byte_names_and_directory_cookies_are_exact_and_deterministic() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let names: [&[u8]; 5] = [b"A", b"a", b"e\xcc\x81", b"\xc3\xa9", b"\xff"];
    let mut inode_by_name = Vec::new();
    for name in names {
        let Reply::Created { entry, .. } = namespace
            .dispatch(
                CALLER,
                Operation::Create {
                    parent: ROOT_INODE,
                    name,
                    mode: 0o600,
                    options: OpenOptions::READ_ONLY,
                    exclusive: true,
                    truncate: false,
                },
            )
            .expect("distinct byte name must be created")
        else {
            panic!("ASSERT: create returned the wrong reply variant");
        };
        inode_by_name.push((name.to_vec(), entry.attr.inode));
    }

    let Reply::Directory(all) = namespace
        .dispatch(
            CALLER,
            Operation::ReadDirectory {
                inode: ROOT_INODE,
                offset: 0,
                acquire_lookup: false,
            },
        )
        .expect("readdir must succeed")
    else {
        panic!("ASSERT: readdir returned the wrong reply variant");
    };
    assert_eq!(all[0].name, b".");
    assert_eq!(all[1].name, b"..");
    let actual_names = all[2..]
        .iter()
        .map(|entry| entry.name.clone())
        .collect::<Vec<_>>();
    let expected_names = inode_by_name
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(actual_names, expected_names);

    for (name, inode) in inode_by_name {
        let Reply::Entry(found) = namespace
            .dispatch(
                CALLER,
                Operation::Lookup {
                    parent: ROOT_INODE,
                    name: &name,
                },
            )
            .expect("lookup must preserve raw bytes")
        else {
            panic!("ASSERT: lookup returned the wrong reply variant");
        };
        assert_eq!(found.attr.inode, inode);
    }

    let Reply::Directory(resumed) = namespace
        .dispatch(
            CALLER,
            Operation::ReadDirectory {
                inode: ROOT_INODE,
                offset: all[2].next_offset,
                acquire_lookup: false,
            },
        )
        .expect("readdir resume must succeed")
    else {
        panic!("ASSERT: readdir returned the wrong reply variant");
    };
    assert_eq!(resumed, all[3..]);

    for invalid in [&b""[..], &b"."[..], &b".."[..], &b"a/b"[..], &b"a\0b"[..]] {
        assert_eq!(
            namespace.dispatch(
                CALLER,
                Operation::Lookup {
                    parent: ROOT_INODE,
                    name: invalid,
                },
            ),
            Err(PosixError::InvalidName)
        );
    }
}

#[test]
fn concurrent_append_writes_are_non_overlapping() {
    const WRITERS: u8 = 8;
    const RECORDS_PER_WRITER: u8 = 32;

    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let append = OpenOptions {
        access: AccessMode::ReadWrite,
        append: true,
    };
    let Reply::Created {
        entry,
        handle: first,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"append.log",
                mode: 0o600,
                options: append,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create must succeed")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    let mut handles = vec![first];
    for _ in 1..WRITERS {
        let Reply::Opened(handle) = namespace
            .dispatch(
                CALLER,
                Operation::Open {
                    inode,
                    options: append,
                    truncate: false,
                },
            )
            .expect("append open must succeed")
        else {
            panic!("ASSERT: open returned the wrong reply variant");
        };
        handles.push(handle);
    }

    std::thread::scope(|scope| {
        for (writer, handle) in handles.iter().copied().enumerate() {
            let namespace = Arc::clone(&namespace);
            scope.spawn(move || {
                for record in 0..RECORDS_PER_WRITER {
                    let frame = [
                        u8::try_from(writer).expect("writer index fits"),
                        record,
                        0x5a,
                        0xa5,
                    ];
                    let reply = namespace.dispatch(
                        CALLER,
                        Operation::Write {
                            inode,
                            handle,
                            offset: 0,
                            data: &frame,
                        },
                    );
                    assert!(matches!(reply, Ok(Reply::Written { bytes: 4, .. })));
                }
            });
        }
    });

    let Reply::Data(bytes) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle: first,
                offset: 0,
                length: u32::MAX,
            },
        )
        .expect("read must succeed")
    else {
        panic!("ASSERT: read returned the wrong reply variant");
    };
    assert_eq!(
        bytes.len(),
        usize::from(WRITERS) * usize::from(RECORDS_PER_WRITER) * 4
    );
    let actual = bytes
        .chunks_exact(4)
        .map(<[u8; 4]>::try_from)
        .collect::<Result<BTreeSet<_>, _>>()
        .expect("frames are exactly four bytes");
    let expected = (0..WRITERS)
        .flat_map(|writer| (0..RECORDS_PER_WRITER).map(move |record| [writer, record, 0x5a, 0xa5]))
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn concurrent_create_has_one_winner_and_expected_errors_do_not_crash() {
    const CONTENDERS: usize = 16;
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig {
        maximum_name_bytes: 8,
        maximum_file_bytes: 4,
    }));
    let barrier = Arc::new(std::sync::Barrier::new(CONTENDERS));
    let outcomes = std::thread::scope(|scope| {
        let workers = (0..CONTENDERS)
            .map(|_| {
                let namespace = Arc::clone(&namespace);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    namespace.dispatch(
                        CALLER,
                        Operation::Create {
                            parent: ROOT_INODE,
                            name: b"same",
                            mode: 0o600,
                            options: OpenOptions::READ_WRITE,
                            exclusive: true,
                            truncate: false,
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("create worker must not panic"))
            .collect::<Vec<_>>()
    });
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(Reply::Created { .. })))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == Err(PosixError::Exists))
            .count(),
        CONTENDERS - 1
    );

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"name-too-long",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        ),
        Err(PosixError::NameTooLong)
    );
    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"same",
            },
        )
        .expect("winning name remains present")
    else {
        panic!("ASSERT: lookup returned wrong reply variant");
    };
    assert_eq!(entry.attr.size, 0);
}

#[test]
fn terabyte_sparse_offsets_consume_only_written_bytes() {
    const TIB: u64 = 1_024 * 1_024 * 1_024 * 1_024;
    let namespace = Namespace::new_volatile(NamespaceConfig {
        maximum_name_bytes: 255,
        maximum_file_bytes: 2 * TIB,
    });
    let Reply::Created {
        entry,
        handle: writer,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"sparse.vm",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create must succeed")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    assert!(matches!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: TIB,
                data: b"X",
            },
        ),
        Ok(Reply::Written { bytes: 1, .. })
    ));
    let Reply::Attr(attr) = namespace
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("getattr must succeed")
    else {
        panic!("ASSERT: getattr returned the wrong reply variant");
    };
    assert_eq!(attr.size, TIB + 1);
    assert_eq!(attr.allocated_bytes, 1);
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle: writer,
                offset: TIB - 4,
                length: 8,
            },
        ),
        Ok(Reply::Data(vec![0, 0, 0, 0, b'X']))
    );

    let Reply::Attr(shrunk) = namespace
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode,
                handle: Some(writer),
                length: TIB,
            },
        )
        .expect("sparse truncate must succeed")
    else {
        panic!("ASSERT: truncate returned the wrong reply variant");
    };
    assert_eq!(shrunk.size, TIB);
    assert_eq!(shrunk.allocated_bytes, 0);
}

#[test]
fn open_or_create_and_truncate_are_one_namespace_operation() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created {
        entry,
        handle: first,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"atomic",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("exclusive create must succeed")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: first,
                offset: 0,
                data: b"old bytes",
            },
        )
        .expect("write must succeed");

    let Reply::Created {
        entry: reopened,
        handle: second,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"atomic",
                mode: 0o777,
                options: OpenOptions::READ_WRITE,
                exclusive: false,
                truncate: true,
            },
        )
        .expect("nonexclusive create must atomically open and truncate")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    assert_eq!(reopened.attr.inode, inode);
    assert_eq!(reopened.attr.size, 0);
    assert_eq!(reopened.attr.mode, 0o600);
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle: second,
                offset: 0,
                length: 32,
            },
        ),
        Ok(Reply::Data(Vec::new()))
    );
}

#[test]
fn lookup_unlink_races_never_assert_and_forget_reclaims_the_inode() {
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    for _ in 0..256 {
        let Reply::Created {
            entry,
            handle: writer,
        } = namespace
            .dispatch(
                CALLER,
                Operation::Create {
                    parent: ROOT_INODE,
                    name: b"raced",
                    mode: 0o600,
                    options: OpenOptions::READ_WRITE,
                    exclusive: true,
                    truncate: false,
                },
            )
            .expect("create must succeed")
        else {
            panic!("ASSERT: create returned the wrong reply variant");
        };
        let inode = entry.attr.inode;
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let lookup_succeeded = std::thread::scope(|scope| {
            let lookup_namespace = Arc::clone(&namespace);
            let lookup_barrier = Arc::clone(&barrier);
            let lookup = scope.spawn(move || {
                lookup_barrier.wait();
                lookup_namespace.dispatch(
                    CALLER,
                    Operation::Lookup {
                        parent: ROOT_INODE,
                        name: b"raced",
                    },
                )
            });
            let unlink_namespace = Arc::clone(&namespace);
            let unlink_barrier = Arc::clone(&barrier);
            let unlink = scope.spawn(move || {
                unlink_barrier.wait();
                unlink_namespace.dispatch(
                    CALLER,
                    Operation::Unlink {
                        parent: ROOT_INODE,
                        name: b"raced",
                    },
                )
            });
            let lookup = lookup.join().expect("lookup race must not panic");
            assert_eq!(
                unlink.join().expect("unlink race must not panic"),
                Ok(Reply::Empty)
            );
            match lookup {
                Ok(Reply::Entry(found)) => {
                    assert_eq!(found.attr.inode, inode);
                    true
                }
                Err(PosixError::NoEntry) => false,
                other => panic!("unexpected lookup race result: {other:?}"),
            }
        });
        assert_eq!(
            namespace.dispatch(
                CALLER,
                Operation::Release {
                    inode,
                    handle: writer
                }
            ),
            Ok(Reply::Empty)
        );
        assert_eq!(
            namespace.dispatch(
                CALLER,
                Operation::Forget {
                    inode,
                    lookup_count: 1 + u64::from(lookup_succeeded),
                },
            ),
            Ok(Reply::Empty)
        );
        assert_eq!(
            namespace.dispatch(CALLER, Operation::GetAttr { inode }),
            Err(PosixError::NoEntry)
        );
    }
}

#[test]
fn sparse_extent_updates_match_a_dense_differential_oracle() {
    const LIMIT: u64 = 4_096;
    let namespace = Namespace::new_volatile(NamespaceConfig {
        maximum_name_bytes: 255,
        maximum_file_bytes: LIMIT,
    });
    let Reply::Created {
        entry,
        handle: writer,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"differential",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create must succeed")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    let mut oracle = Vec::new();
    let mut state = 0x8f3c_2a91_7b6d_405e_u64;
    for step in 0..4_096_u64 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        if state.is_multiple_of(5) {
            let length = usize::try_from((state >> 17) % (LIMIT + 1)).expect("bounded length");
            namespace
                .dispatch(
                    CALLER,
                    Operation::SetLength {
                        inode,
                        handle: Some(writer),
                        length: u64::try_from(length).expect("length fits"),
                    },
                )
                .expect("truncate must succeed");
            oracle.resize(length, 0);
        } else {
            let offset = usize::try_from((state >> 11) % LIMIT).expect("bounded offset");
            let length = usize::try_from(((state >> 29) % 32) + 1).expect("bounded write");
            let end = (offset + length).min(usize::try_from(LIMIT).expect("limit fits"));
            let data = (offset..end)
                .map(|index| u8::try_from((index as u64 + step) % 251).expect("byte fits"))
                .collect::<Vec<_>>();
            namespace
                .dispatch(
                    CALLER,
                    Operation::Write {
                        inode,
                        handle: writer,
                        offset: u64::try_from(offset).expect("offset fits"),
                        data: &data,
                    },
                )
                .expect("write must succeed");
            if end > oracle.len() {
                oracle.resize(end, 0);
            }
            oracle[offset..end].copy_from_slice(&data);
        }

        let Reply::Data(actual) = namespace
            .dispatch(
                CALLER,
                Operation::Read {
                    inode,
                    handle: writer,
                    offset: 0,
                    length: u32::try_from(LIMIT).expect("limit fits"),
                },
            )
            .expect("read must succeed")
        else {
            panic!("ASSERT: read returned the wrong reply variant");
        };
        assert_eq!(actual, oracle, "differential mismatch at step {step}");
    }
}

#[test]
fn directory_pages_are_bounded_without_skipping_static_entries() {
    const FILES: u32 = 600;
    const PAGE_LIMIT: usize = 256;

    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let mut expected = Vec::new();
    for index in 0..FILES {
        let name = format!("file-{index:04}").into_bytes();
        let Reply::Created { entry, handle } = namespace
            .dispatch(
                CALLER,
                Operation::Create {
                    parent: ROOT_INODE,
                    name: &name,
                    mode: 0o600,
                    options: OpenOptions::READ_WRITE,
                    exclusive: true,
                    truncate: false,
                },
            )
            .expect("create must succeed")
        else {
            panic!("ASSERT: create returned the wrong reply variant");
        };
        assert_eq!(
            namespace.dispatch(
                CALLER,
                Operation::Release {
                    inode: entry.attr.inode,
                    handle,
                },
            ),
            Ok(Reply::Empty)
        );
        expected.push(name);
    }

    let mut offset = 2_i64;
    let mut observed = Vec::new();
    loop {
        let Reply::Directory(entries) = namespace
            .dispatch(
                CALLER,
                Operation::ReadDirectory {
                    inode: ROOT_INODE,
                    offset,
                    acquire_lookup: false,
                },
            )
            .expect("directory page must succeed")
        else {
            panic!("ASSERT: readdir returned the wrong reply variant");
        };
        assert!(entries.len() <= PAGE_LIMIT);
        let Some(last) = entries.last() else {
            break;
        };
        assert!(last.next_offset > offset);
        offset = last.next_offset;
        observed.extend(entries.into_iter().map(|entry| entry.name));
    }
    assert_eq!(observed, expected);
}

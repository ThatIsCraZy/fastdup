use fastdup_posix::{
    FallocateMode, HandleId, InodeId, Namespace, NamespaceConfig, OpenOptions, Operation,
    PosixError, ROOT_INODE, Reply, RequestContext, SeekKind,
};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 77,
};

fn create(namespace: &Namespace, name: &[u8]) -> (InodeId, HandleId) {
    let Reply::Created { entry, handle } = namespace
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
        .expect("create file")
    else {
        panic!("ASSERT: create returned the wrong reply");
    };
    (entry.attr.inode, handle)
}

fn write(namespace: &Namespace, inode: InodeId, handle: HandleId, offset: u64, bytes: &[u8]) {
    assert!(matches!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset,
                data: bytes,
            },
        ),
        Ok(Reply::Written { .. })
    ));
}

fn fallocate(
    namespace: &Namespace,
    inode: InodeId,
    handle: HandleId,
    offset: u64,
    length: u64,
    mode: FallocateMode,
) {
    assert!(matches!(
        namespace.dispatch(
            CALLER,
            Operation::Fallocate {
                inode,
                handle,
                offset,
                length,
                mode,
            },
        ),
        Ok(Reply::Attr(_))
    ));
}

fn read_all(namespace: &Namespace, inode: InodeId, handle: HandleId, length: usize) -> Vec<u8> {
    let Reply::Data(bytes) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle,
                offset: 0,
                length: u32::try_from(length).expect("test file fits u32"),
            },
        )
        .expect("read file")
    else {
        panic!("ASSERT: read returned the wrong reply");
    };
    bytes
}

fn seek(
    namespace: &Namespace,
    inode: InodeId,
    handle: HandleId,
    offset: u64,
    kind: SeekKind,
) -> Result<u64, PosixError> {
    match namespace.dispatch(
        CALLER,
        Operation::Seek {
            inode,
            handle,
            offset,
            kind,
        },
    )? {
        Reply::Offset(found) => Ok(found),
        _ => panic!("ASSERT: seek returned the wrong reply"),
    }
}

#[test]
fn allocation_preserves_data_and_hole_zeroing_remains_allocated() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let (inode, handle) = create(&namespace, b"allocated");
    write(&namespace, inode, handle, 4, b"DATA");

    fallocate(
        &namespace,
        inode,
        handle,
        0,
        12,
        FallocateMode::Allocate { keep_size: false },
    );
    assert_eq!(
        read_all(&namespace, inode, handle, 12),
        b"\0\0\0\0DATA\0\0\0\0"
    );
    let Reply::Attr(attr) = namespace
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("get attributes")
    else {
        panic!("ASSERT: getattr returned the wrong reply");
    };
    assert_eq!((attr.size, attr.allocated_bytes), (12, 12));
    assert_eq!(seek(&namespace, inode, handle, 0, SeekKind::Data), Ok(0));
    assert_eq!(seek(&namespace, inode, handle, 0, SeekKind::Hole), Ok(12));

    fallocate(&namespace, inode, handle, 2, 8, FallocateMode::PunchHole);
    assert_eq!(read_all(&namespace, inode, handle, 12), vec![0; 12]);
    let Reply::Attr(attr) = namespace
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("get punched attributes")
    else {
        panic!("ASSERT: getattr returned the wrong reply");
    };
    assert_eq!(attr.allocated_bytes, 4);
    assert_eq!(seek(&namespace, inode, handle, 0, SeekKind::Hole), Ok(2));
    assert_eq!(seek(&namespace, inode, handle, 2, SeekKind::Data), Ok(10));

    fallocate(
        &namespace,
        inode,
        handle,
        3,
        4,
        FallocateMode::ZeroRange { keep_size: true },
    );
    let Reply::Attr(attr) = namespace
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("get zeroed attributes")
    else {
        panic!("ASSERT: getattr returned the wrong reply");
    };
    assert_eq!((attr.size, attr.allocated_bytes), (12, 8));
    assert_eq!(seek(&namespace, inode, handle, 2, SeekKind::Data), Ok(3));
    assert_eq!(seek(&namespace, inode, handle, 3, SeekKind::Hole), Ok(7));
    assert_eq!(
        seek(&namespace, inode, handle, 12, SeekKind::Data),
        Err(PosixError::NoSuchAddress)
    );
}

#[test]
fn collapse_and_insert_shift_sparse_layout_without_payload_materialization() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let (inode, handle) = create(&namespace, b"splice");
    write(&namespace, inode, handle, 0, b"abc");
    write(&namespace, inode, handle, 8, b"XYZ");
    let resident_before = namespace.checkpointable_dirty_payload_bytes();

    fallocate(
        &namespace,
        inode,
        handle,
        2,
        4,
        FallocateMode::CollapseRange,
    );
    assert_eq!(read_all(&namespace, inode, handle, 7), b"ab\0\0XYZ");
    assert_eq!(
        namespace.checkpointable_dirty_payload_bytes(),
        resident_before - 1
    );

    fallocate(&namespace, inode, handle, 1, 3, FallocateMode::InsertRange);
    assert_eq!(read_all(&namespace, inode, handle, 10), b"a\0\0\0b\0\0XYZ");
    assert_eq!(
        namespace.checkpointable_dirty_payload_bytes(),
        resident_before - 1
    );
    let Reply::Attr(attr) = namespace
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("get spliced attributes")
    else {
        panic!("ASSERT: getattr returned the wrong reply");
    };
    assert_eq!((attr.size, attr.allocated_bytes), (10, 5));
    assert_eq!(seek(&namespace, inode, handle, 0, SeekKind::Hole), Ok(1));
    assert_eq!(seek(&namespace, inode, handle, 1, SeekKind::Data), Ok(4));
}

#[test]
fn terabyte_zero_range_is_one_metadata_extent() {
    const TIB: u64 = 1_024 * 1_024 * 1_024 * 1_024;
    let namespace = Namespace::new_volatile(NamespaceConfig {
        maximum_name_bytes: 255,
        maximum_file_bytes: 2 * TIB,
    });
    let (inode, handle) = create(&namespace, b"zero-fill");
    fallocate(
        &namespace,
        inode,
        handle,
        0,
        TIB,
        FallocateMode::ZeroRange { keep_size: false },
    );
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 0);
    let Reply::Attr(attr) = namespace
        .dispatch(CALLER, Operation::GetAttr { inode })
        .expect("get fill attributes")
    else {
        panic!("ASSERT: getattr returned the wrong reply");
    };
    assert_eq!((attr.size, attr.allocated_bytes), (TIB, TIB));
    assert_eq!(
        seek(&namespace, inode, handle, TIB - 1, SeekKind::Data),
        Ok(TIB - 1)
    );
    assert_eq!(seek(&namespace, inode, handle, 0, SeekKind::Hole), Ok(TIB));
}

#[test]
fn invalid_ranges_and_read_only_handles_fail_without_mutation() {
    let namespace = Namespace::new_volatile(NamespaceConfig {
        maximum_name_bytes: 255,
        maximum_file_bytes: 16,
    });
    let (inode, writer) = create(&namespace, b"errors");
    write(&namespace, inode, writer, 0, b"abcd");
    let Reply::Opened(reader) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open reader")
    else {
        panic!("ASSERT: open returned the wrong reply");
    };
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Fallocate {
                inode,
                handle: reader,
                offset: 0,
                length: 1,
                mode: FallocateMode::PunchHole,
            },
        ),
        Err(PosixError::BadHandle)
    );
    for (offset, length, mode, error) in [
        (0, 0, FallocateMode::PunchHole, PosixError::InvalidArgument),
        (
            2,
            2,
            FallocateMode::CollapseRange,
            PosixError::InvalidArgument,
        ),
        (
            4,
            1,
            FallocateMode::InsertRange,
            PosixError::InvalidArgument,
        ),
        (
            3,
            14,
            FallocateMode::ZeroRange { keep_size: false },
            PosixError::FileTooLarge,
        ),
    ] {
        assert_eq!(
            namespace.dispatch(
                CALLER,
                Operation::Fallocate {
                    inode,
                    handle: writer,
                    offset,
                    length,
                    mode,
                },
            ),
            Err(error)
        );
    }
    assert_eq!(read_all(&namespace, inode, writer, 4), b"abcd");
}

#[test]
#[allow(clippy::too_many_lines)]
fn deterministic_sparse_operations_match_a_byte_and_allocation_oracle() {
    let namespace = Namespace::new_volatile(NamespaceConfig {
        maximum_name_bytes: 255,
        maximum_file_bytes: 256,
    });
    let (inode, handle) = create(&namespace, b"oracle");
    let mut oracle = Vec::<Option<u8>>::new();
    let mut state = 0x91e1_0da5_u64;

    for step in 0..600_u64 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let operation = (state >> 32) % 7;
        let current = oracle.len();
        match operation {
            0 if current < 248 => {
                let offset = usize::try_from(state % u64::try_from(current + 5).unwrap()).unwrap();
                let length = usize::try_from((state >> 8) % 4 + 1).unwrap();
                if offset + length <= 256 {
                    let bytes = (0..length)
                        .map(|index| {
                            u8::try_from((step + u64::try_from(index).unwrap()) % 251 + 1).unwrap()
                        })
                        .collect::<Vec<_>>();
                    write(
                        &namespace,
                        inode,
                        handle,
                        u64::try_from(offset).unwrap(),
                        &bytes,
                    );
                    oracle.resize(oracle.len().max(offset + length), None);
                    for (index, byte) in bytes.into_iter().enumerate() {
                        oracle[offset + index] = Some(byte);
                    }
                }
            }
            1 if current > 0 => {
                let offset = usize::try_from(state % u64::try_from(current + 3).unwrap()).unwrap();
                let length = usize::try_from((state >> 8) % 6 + 1).unwrap();
                fallocate(
                    &namespace,
                    inode,
                    handle,
                    offset as u64,
                    length as u64,
                    FallocateMode::PunchHole,
                );
                for slot in oracle.iter_mut().skip(offset).take(length) {
                    *slot = None;
                }
            }
            2 | 3 if current < 248 => {
                let keep_size = operation == 3;
                let offset = usize::try_from(state % u64::try_from(current + 5).unwrap()).unwrap();
                let length = usize::try_from((state >> 8) % 6 + 1).unwrap();
                if keep_size || offset + length <= 256 {
                    fallocate(
                        &namespace,
                        inode,
                        handle,
                        offset as u64,
                        length as u64,
                        FallocateMode::ZeroRange { keep_size },
                    );
                    if !keep_size {
                        oracle.resize(oracle.len().max(offset + length), None);
                    }
                    for slot in oracle.iter_mut().skip(offset).take(length) {
                        *slot = Some(0);
                    }
                }
            }
            4 if current < 248 => {
                let offset = usize::try_from(state % u64::try_from(current + 5).unwrap()).unwrap();
                let length = usize::try_from((state >> 8) % 6 + 1).unwrap();
                if offset + length <= 256 {
                    fallocate(
                        &namespace,
                        inode,
                        handle,
                        offset as u64,
                        length as u64,
                        FallocateMode::Allocate { keep_size: false },
                    );
                    oracle.resize(oracle.len().max(offset + length), None);
                    for slot in oracle.iter_mut().skip(offset).take(length) {
                        if slot.is_none() {
                            *slot = Some(0);
                        }
                    }
                }
            }
            5 if current >= 2 => {
                let offset = usize::try_from(state % u64::try_from(current - 1).unwrap()).unwrap();
                let maximum = current - offset - 1;
                let length =
                    1 + usize::try_from((state >> 8) % u64::try_from(maximum).unwrap()).unwrap();
                fallocate(
                    &namespace,
                    inode,
                    handle,
                    offset as u64,
                    length as u64,
                    FallocateMode::CollapseRange,
                );
                oracle.drain(offset..offset + length);
            }
            6 if current > 0 && current < 252 => {
                let offset = usize::try_from(state % u64::try_from(current).unwrap()).unwrap();
                let length = usize::try_from((state >> 8) % 4 + 1).unwrap();
                if current + length <= 256 {
                    fallocate(
                        &namespace,
                        inode,
                        handle,
                        offset as u64,
                        length as u64,
                        FallocateMode::InsertRange,
                    );
                    oracle.splice(offset..offset, std::iter::repeat_n(None, length));
                }
            }
            _ => {}
        }

        let expected = oracle
            .iter()
            .map(|byte| byte.unwrap_or(0))
            .collect::<Vec<_>>();
        assert_eq!(
            read_all(&namespace, inode, handle, oracle.len()),
            expected,
            "step {step}"
        );
        let Reply::Attr(attr) = namespace
            .dispatch(CALLER, Operation::GetAttr { inode })
            .expect("get oracle attributes")
        else {
            panic!("ASSERT: getattr returned the wrong reply");
        };
        assert_eq!(attr.size, oracle.len() as u64, "step {step}");
        assert_eq!(
            attr.allocated_bytes,
            oracle.iter().filter(|byte| byte.is_some()).count() as u64,
            "step {step}"
        );
        for offset in 0..=oracle.len() {
            let expected_data = oracle
                .iter()
                .enumerate()
                .skip(offset)
                .find_map(|(index, byte)| byte.is_some().then_some(index as u64));
            let expected_hole = if offset == oracle.len() {
                None
            } else {
                oracle
                    .iter()
                    .enumerate()
                    .skip(offset)
                    .find_map(|(index, byte)| byte.is_none().then_some(index as u64))
                    .or(Some(oracle.len() as u64))
            };
            assert_eq!(
                seek(&namespace, inode, handle, offset as u64, SeekKind::Data),
                expected_data.ok_or(PosixError::NoSuchAddress),
                "data seek at step {step}, offset {offset}"
            );
            assert_eq!(
                seek(&namespace, inode, handle, offset as u64, SeekKind::Hole),
                expected_hole.ok_or(PosixError::NoSuchAddress),
                "hole seek at step {step}, offset {offset}"
            );
        }
    }
}

use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use fastdup_format::{
    ChunkId, ExactIndexRunSetId, SIMILARITY_BUCKET_REFERENCES_PER_PAGE,
    SIMILARITY_INDEX_ENTRIES_PER_PAGE, SIMILARITY_INDEX_HEADER_BYTES, SIMILARITY_INDEX_PAGE_BYTES,
    SimilarityBucketKey, SimilarityBucketPage, SimilarityIndexEntry, SimilarityIndexFamilyError,
    SimilarityIndexFormatError, SimilarityIndexPage, SimilarityIndexPartitionRef,
    SimilarityIndexRun, SimilarityIndexRunDescriptor, SimilarityIndexRunFamily,
    SimilarityIndexRunStreamEncoder,
};

use crate::StorageIo;
use crate::reduction_similarity::{
    MAX_SIMILARITY_CANDIDATES, SIMILARITY_BUCKET_PROFILE_V1, SIMILARITY_PROFILE_V1,
    SimilarityError, SimilarityFingerprint,
};
use crate::similarity_external_sort::{
    BuiltSimilarityPartition, PartitionedSimilarityBuild, SimilarityEntryStager,
    write_partitioned_runs,
};
use crate::similarity_mmap::ImmutableSimilarityRun;

/// The complete pool fingerprint algorithm written by this implementation.
pub const SIMILARITY_FINGERPRINT_PROFILE_V1: u16 = SIMILARITY_PROFILE_V1;
/// The bounded 64-representative bucket policy written by this implementation.
pub const SIMILARITY_REPRESENTATIVE_PROFILE_V1: u16 = SIMILARITY_BUCKET_PROFILE_V1;

/// Durable immutable Similarity snapshot publication and streaming rebuild.
#[derive(Clone, Debug)]
pub struct SimilarityIndexRepository<I> {
    storage: I,
    publish_lock: Arc<Mutex<()>>,
}

impl<I: Clone + StorageIo> SimilarityIndexRepository<I> {
    #[must_use]
    pub fn new(storage: I) -> Self {
        Self {
            storage,
            publish_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Durably publishes one complete immutable pool snapshot.
    ///
    /// An idempotent retry accepts an existing generation only when profiles,
    /// length, and the complete run hash agree.
    ///
    /// # Errors
    ///
    /// Returns format, I/O, profile, collision, reread, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics if the process-local writer lock is poisoned.
    pub fn publish(
        &self,
        run: &SimilarityIndexRun,
    ) -> Result<SimilarityIndexRunDescriptor, SimilarityIndexStoreError> {
        require_v1_profiles(run.fingerprint_profile(), run.bucket_profile())?;
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Similarity Index publication lock poisoned");
        let published_name = published_name(run.generation());
        let temporary_name = format!(".{published_name}.building");

        if self.storage.exists(&published_name)? {
            let expected = stream_similarity_run(run, |_, _| Ok(()))?;
            let observed = self.audit_named(&published_name, |_| Ok(()))?;
            verify_expected_descriptor(expected, observed)?;
            self.storage.sync_root()?;
            return Ok(observed);
        }

        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let expected = stream_similarity_run(run, |offset, page| {
            self.storage.write_at(&temporary_name, offset, page)?;
            Ok(())
        })?;
        self.storage
            .set_len(&temporary_name, expected.file_length())?;
        let observed = self.audit_named(&temporary_name, |_| Ok(()))?;
        verify_expected_descriptor(expected, observed)?;
        self.storage.sync_file(&temporary_name)?;
        match self
            .storage
            .publish_noreplace(&temporary_name, &published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let raced = self.audit_named(&published_name, |_| Ok(()))?;
                verify_expected_descriptor(expected, raced)?;
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok(observed)
    }

    /// Externally sorts and durably publishes an unsorted entry stream.
    ///
    /// Sorting and representative selection use bounded in-memory chunks and
    /// private on-storage spools. Global representative selection is identical
    /// to a canonical [`SimilarityIndexRun`] built from the same entries.
    ///
    /// # Errors
    ///
    /// Returns format, I/O, profile, duplicate-identity, reread, cleanup, or
    /// durability errors.
    ///
    /// # Panics
    ///
    /// Panics if the process-local writer lock is poisoned.
    pub fn publish_entries<E>(
        &self,
        generation: u64,
        entries: E,
    ) -> Result<SimilarityIndexRunFamily, SimilarityIndexStoreError>
    where
        E: IntoIterator<Item = SimilarityIndexEntry>,
    {
        let build = write_partitioned_runs(&self.storage, generation, entries)?;
        let staged = {
            let _guard = self
                .publish_lock
                .lock()
                .expect("ASSERT: Similarity Index publication lock poisoned");
            self.stage_built_family(generation, &build, None)?
        };
        self.activate_staged_family(staged)
    }

    pub(crate) fn entry_stager(&self, generation: u64) -> SimilarityEntryStager<'_, I> {
        SimilarityEntryStager::new(&self.storage, generation)
    }

    pub(crate) fn finish_staged_entries(
        &self,
        generation: u64,
        stager: SimilarityEntryStager<'_, I>,
        source_exact_run_set_id: ExactIndexRunSetId,
    ) -> Result<StagedSimilarityIndex, SimilarityIndexStoreError> {
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Similarity Index publication lock poisoned");
        if stager.is_empty() {
            return Self::stage_empty_family(generation, source_exact_run_set_id);
        }
        let build = stager.finish()?;
        self.stage_built_family(generation, &build, Some(source_exact_run_set_id))
    }

    fn stage_built_family(
        &self,
        generation: u64,
        build: &PartitionedSimilarityBuild,
        source_exact_run_set_id: Option<ExactIndexRunSetId>,
    ) -> Result<StagedSimilarityIndex, SimilarityIndexStoreError> {
        let family_temporary_name = format!(".similarity-family-{generation:016x}.building");
        let mut cleanup_names = build
            .partitions
            .iter()
            .map(|partition| partition.temporary_name.clone())
            .collect::<Vec<_>>();
        cleanup_names.push(family_temporary_name.clone());
        let _cleanup = TemporarySimilarityFiles::new(&self.storage, cleanup_names);
        let family_name = family_name(generation);
        let partition_count = u16::try_from(build.partitions.len())
            .map_err(|_| SimilarityIndexStoreError::TooManyPartitions)?;
        let mut references = Vec::new();
        references
            .try_reserve_exact(build.partitions.len())
            .map_err(|_| SimilarityIndexStoreError::OutOfMemory)?;
        for (ordinal, partition) in build.partitions.iter().enumerate() {
            let (observed, minimum_bucket_key, maximum_bucket_key) =
                self.audit_partition_named(&partition.temporary_name)?;
            verify_expected_descriptor(partition.descriptor, observed)?;
            if minimum_bucket_key != partition.minimum_bucket_key
                || maximum_bucket_key != partition.maximum_bucket_key
            {
                return Err(SimilarityIndexStoreError::IndexCorruption);
            }
            self.storage.sync_file(&partition.temporary_name)?;
            references.push(SimilarityIndexPartitionRef::new(
                generation,
                u16::try_from(ordinal).map_err(|_| SimilarityIndexStoreError::TooManyPartitions)?,
                partition_count,
                observed,
                minimum_bucket_key,
                maximum_bucket_key,
            )?);
        }
        let family = match source_exact_run_set_id {
            Some(id) => SimilarityIndexRunFamily::new_bound(
                SIMILARITY_FINGERPRINT_PROFILE_V1,
                SIMILARITY_REPRESENTATIVE_PROFILE_V1,
                generation,
                build.logical_entry_count,
                id,
                references,
            )?,
            None => SimilarityIndexRunFamily::new(
                SIMILARITY_FINGERPRINT_PROFILE_V1,
                SIMILARITY_REPRESENTATIVE_PROFILE_V1,
                generation,
                build.logical_entry_count,
                references,
            )?,
        };
        let encoded_family = family.encode()?;

        if self.storage.exists(&family_name)? {
            let observed = self.read_family(&family_name)?;
            if observed != family || self.storage.read(&family_name)? != encoded_family {
                return Err(SimilarityIndexStoreError::PublishVerificationMismatch);
            }
            self.verify_published_partitions(&build.partitions)?;
            self.storage.sync_root()?;
            return Ok(StagedSimilarityIndex {
                family: observed,
                encoded_family,
                family_temporary_name,
            });
        }

        self.publish_physical_partitions(&build.partitions)?;
        self.storage.sync_root()?;
        Ok(StagedSimilarityIndex {
            family,
            encoded_family,
            family_temporary_name,
        })
    }

    fn stage_empty_family(
        generation: u64,
        source_exact_run_set_id: ExactIndexRunSetId,
    ) -> Result<StagedSimilarityIndex, SimilarityIndexStoreError> {
        let family = SimilarityIndexRunFamily::new_bound(
            SIMILARITY_FINGERPRINT_PROFILE_V1,
            SIMILARITY_REPRESENTATIVE_PROFILE_V1,
            generation,
            0,
            source_exact_run_set_id,
            Vec::new(),
        )?;
        let encoded_family = family.encode()?;
        Ok(StagedSimilarityIndex {
            family,
            encoded_family,
            family_temporary_name: format!(".similarity-family-{generation:016x}.building"),
        })
    }

    /// Returns the largest generation named by any published Similarity
    /// object, including orphan partitions left by an interrupted rebuild.
    pub(crate) fn discover_generation_high_water(
        &self,
    ) -> Result<Option<u64>, SimilarityIndexStoreError> {
        let mut high_water = None;
        for name in self.storage.list_names()? {
            let generation = parse_published_name(&name)?
                .or(parse_family_name(&name)?)
                .or(parse_partition_name(&name)?);
            if let Some(generation) = generation {
                high_water =
                    Some(high_water.map_or(generation, |value: u64| value.max(generation)));
            }
        }
        Ok(high_water)
    }

    pub(crate) fn activate_staged_family(
        &self,
        staged: StagedSimilarityIndex,
    ) -> Result<SimilarityIndexRunFamily, SimilarityIndexStoreError> {
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Similarity Index publication lock poisoned");
        self.verify_family_partitions(&staged.family)?;
        let family_name = family_name(staged.family.generation());
        let _cleanup = TemporarySimilarityFiles::new(
            &self.storage,
            vec![staged.family_temporary_name.clone()],
        );
        self.publish_family_manifest(
            &family_name,
            &staged.family_temporary_name,
            &staged.family,
            &staged.encoded_family,
        )?;
        Ok(staged.family)
    }

    fn publish_physical_partitions(
        &self,
        partitions: &[BuiltSimilarityPartition],
    ) -> Result<(), SimilarityIndexStoreError> {
        for partition in partitions {
            if self.storage.exists(&partition.published_name)? {
                self.verify_built_partition(&partition.published_name, partition)?;
                remove_if_present(&self.storage, &partition.temporary_name)?;
                continue;
            }
            match self
                .storage
                .publish_noreplace(&partition.temporary_name, &partition.published_name)
            {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    self.verify_built_partition(&partition.published_name, partition)?;
                    remove_if_present(&self.storage, &partition.temporary_name)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn verify_published_partitions(
        &self,
        partitions: &[BuiltSimilarityPartition],
    ) -> Result<(), SimilarityIndexStoreError> {
        for partition in partitions {
            self.verify_built_partition(&partition.published_name, partition)?;
        }
        Ok(())
    }

    fn verify_family_partitions(
        &self,
        family: &SimilarityIndexRunFamily,
    ) -> Result<(), SimilarityIndexStoreError> {
        for reference in family.partitions().iter().copied() {
            let name = partition_name(family.generation(), reference.partition_ordinal());
            let (descriptor, minimum_bucket_key, maximum_bucket_key) =
                self.audit_partition_named(&name)?;
            verify_partition_reference(
                reference,
                descriptor,
                minimum_bucket_key,
                maximum_bucket_key,
            )?;
        }
        Ok(())
    }

    fn verify_built_partition(
        &self,
        name: &str,
        expected: &BuiltSimilarityPartition,
    ) -> Result<(), SimilarityIndexStoreError> {
        let (descriptor, minimum_bucket_key, maximum_bucket_key) =
            self.audit_partition_named(name)?;
        verify_expected_descriptor(expected.descriptor, descriptor)?;
        if minimum_bucket_key != expected.minimum_bucket_key
            || maximum_bucket_key != expected.maximum_bucket_key
        {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        Ok(())
    }

    fn publish_family_manifest(
        &self,
        published_name: &str,
        temporary_name: &str,
        family: &SimilarityIndexRunFamily,
        encoded: &[u8],
    ) -> Result<(), SimilarityIndexStoreError> {
        ensure_object(&self.storage, temporary_name)?;
        self.storage.write_at(temporary_name, 0, encoded)?;
        self.storage.set_len(
            temporary_name,
            u64::try_from(encoded.len()).map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
        )?;
        if self.storage.read(temporary_name)? != encoded
            || self.read_family(temporary_name)? != *family
        {
            return Err(SimilarityIndexStoreError::PublishVerificationMismatch);
        }
        self.storage.sync_file(temporary_name)?;
        match self
            .storage
            .publish_noreplace(temporary_name, published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if self.read_family(published_name)? != *family {
                    return Err(SimilarityIndexStoreError::PublishVerificationMismatch);
                }
                remove_if_present(&self.storage, temporary_name)?;
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok(())
    }

    /// Verifies and opens the newest complete v1-profile pool snapshot.
    ///
    /// Pool buckets remain in the immutable format-v2 Run. Only bounded hot
    /// pages are admitted to RAM. A Similarity result still needs an Exact
    /// Index lookup before the Base bytes may be used.
    ///
    /// # Errors
    ///
    /// Returns I/O, envelope, page, hash, profile, identity, index-invariant,
    /// or checked-counter failures. It never falls back to an older snapshot
    /// when the newest published generation is corrupt.
    pub fn recover_latest(
        &self,
    ) -> Result<Option<RecoveredSimilarityIndex<I>>, SimilarityIndexStoreError> {
        let Some(publication) = self.latest_published()? else {
            return Ok(None);
        };
        let (generation, entries_streamed, buckets, source_exact_run_set_id, partitions) =
            match publication {
                LatestSimilarityPublication::Legacy { generation, name } => {
                    let mut entries_streamed = 0_u64;
                    let verified = self.recover_partition_named(&name)?;
                    let descriptor = verified.descriptor;
                    entries_streamed = entries_streamed
                        .checked_add(
                            u64::try_from(descriptor.entry_count())
                                .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
                        )
                        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
                    require_v1_profiles(
                        descriptor.fingerprint_profile(),
                        descriptor.bucket_profile(),
                    )?;
                    if descriptor.generation() != generation {
                        return Err(SimilarityIndexStoreError::IdentityMismatch);
                    }
                    let buckets = u64::try_from(descriptor.bucket_count())
                        .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
                    let partition = RecoveredSimilarityPartition {
                        name,
                        descriptor,
                        minimum_bucket_key: verified.minimum_bucket_key,
                        maximum_bucket_key: verified.maximum_bucket_key,
                        mapping: verified.mapping,
                        page_cache: Arc::new(SimilarityPageCache::new()),
                    };
                    (generation, entries_streamed, buckets, None, vec![partition])
                }
                LatestSimilarityPublication::Family { generation, name } => {
                    let family = self.read_family(&name)?;
                    require_v1_profiles(family.fingerprint_profile(), family.bucket_profile())?;
                    if family.generation() != generation {
                        return Err(SimilarityIndexStoreError::IdentityMismatch);
                    }
                    let mut partitions = Vec::new();
                    partitions
                        .try_reserve_exact(family.partitions().len())
                        .map_err(|_| SimilarityIndexStoreError::OutOfMemory)?;
                    let mut buckets = 0_u64;
                    for reference in family.partitions().iter().copied() {
                        let partition_name =
                            partition_name(generation, reference.partition_ordinal());
                        let verified = self.recover_partition_named(&partition_name)?;
                        let descriptor = verified.descriptor;
                        verify_partition_reference(
                            reference,
                            descriptor,
                            verified.minimum_bucket_key,
                            verified.maximum_bucket_key,
                        )?;
                        buckets = buckets
                            .checked_add(reference.bucket_count())
                            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
                        partitions.push(RecoveredSimilarityPartition {
                            name: partition_name,
                            descriptor,
                            minimum_bucket_key: verified.minimum_bucket_key,
                            maximum_bucket_key: verified.maximum_bucket_key,
                            mapping: verified.mapping,
                            page_cache: Arc::new(SimilarityPageCache::new()),
                        });
                    }
                    (
                        generation,
                        family.logical_entry_count(),
                        buckets,
                        family.source_exact_run_set_id(),
                        partitions,
                    )
                }
            };
        let mapped_partitions = partitions
            .iter()
            .filter(|partition| partition.mapping.is_some())
            .count();
        let read_mode = match mapped_partitions {
            0 => SimilarityIndexReadMode::ReadExactAt,
            count if count == partitions.len() => SimilarityIndexReadMode::Mmap,
            _ => return Err(SimilarityIndexStoreError::IdentityMismatch),
        };
        let status = SimilarityIndexRebuildStatus {
            generation,
            entries_streamed,
            resident_representatives: 0,
            buckets,
            read_mode,
            source_exact_run_set_id,
        };
        Ok(Some(RecoveredSimilarityIndex {
            storage: self.storage.clone(),
            partitions: partitions.into_boxed_slice(),
            status,
        }))
    }

    /// Recovers only a family produced from the supplied active Exact Run Set.
    ///
    /// Legacy, independently published, and snapshots bound to an older Exact
    /// generation are not selected; this is the fail-closed reader seam for
    /// paired advanced-reduction state.
    ///
    /// # Errors
    ///
    /// Returns recovery failures. An unbound or different Exact identity is a
    /// safe `None`, because it is an expected crash state during replacement.
    pub fn recover_latest_for_exact(
        &self,
        exact_run_set_id: ExactIndexRunSetId,
    ) -> Result<Option<RecoveredSimilarityIndex<I>>, SimilarityIndexStoreError> {
        let recovered = self.recover_latest()?;
        if recovered
            .as_ref()
            .is_some_and(|index| index.status().source_exact_run_set_id() != Some(exact_run_set_id))
        {
            return Ok(None);
        }
        Ok(recovered)
    }

    /// Streams and verifies the newest complete Similarity snapshot without
    /// constructing query state.
    ///
    /// This is the offline-scrub seam for Header/Footer identity, page CRCs,
    /// reserved fields, profile consistency, canonical cross-page ordering,
    /// entry count, and the complete-file run hash.
    ///
    /// # Errors
    ///
    /// Returns I/O, envelope, page, hash, profile, identity, or checked-counter
    /// failures. A corrupt newest generation is reported, never skipped.
    pub fn audit_latest(
        &self,
    ) -> Result<Option<SimilarityIndexAuditStatus>, SimilarityIndexStoreError> {
        let Some(publication) = self.latest_published()? else {
            return Ok(None);
        };
        self.audit_publication(publication)
    }

    fn audit_publication(
        &self,
        publication: LatestSimilarityPublication,
    ) -> Result<Option<SimilarityIndexAuditStatus>, SimilarityIndexStoreError> {
        match publication {
            LatestSimilarityPublication::Legacy { generation, name } => {
                let mut entries_verified = 0_u64;
                let descriptor = self.audit_named(&name, |_| {
                    entries_verified = entries_verified
                        .checked_add(1)
                        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
                    Ok(())
                })?;
                require_v1_profiles(
                    descriptor.fingerprint_profile(),
                    descriptor.bucket_profile(),
                )?;
                if descriptor.generation() != generation
                    || usize::try_from(entries_verified).ok() != Some(descriptor.entry_count())
                {
                    return Err(SimilarityIndexStoreError::IdentityMismatch);
                }
                Ok(Some(SimilarityIndexAuditStatus {
                    generation,
                    entries_verified,
                    pages_verified: u64::try_from(
                        descriptor
                            .page_count()
                            .checked_add(descriptor.bucket_page_count())
                            .ok_or(SimilarityIndexStoreError::CounterOverflow)?,
                    )
                    .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
                    run_hash: descriptor.run_hash(),
                }))
            }
            LatestSimilarityPublication::Family { generation, name } => {
                let family = self.read_family(&name)?;
                require_v1_profiles(family.fingerprint_profile(), family.bucket_profile())?;
                if family.generation() != generation {
                    return Err(SimilarityIndexStoreError::IdentityMismatch);
                }
                let mut pages_verified = 0_u64;
                for reference in family.partitions().iter().copied() {
                    let name = partition_name(generation, reference.partition_ordinal());
                    let (descriptor, minimum_bucket_key, maximum_bucket_key) =
                        self.audit_partition_named(&name)?;
                    verify_partition_reference(
                        reference,
                        descriptor,
                        minimum_bucket_key,
                        maximum_bucket_key,
                    )?;
                    pages_verified = pages_verified
                        .checked_add(
                            u64::try_from(
                                descriptor
                                    .page_count()
                                    .checked_add(descriptor.bucket_page_count())
                                    .ok_or(SimilarityIndexStoreError::CounterOverflow)?,
                            )
                            .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
                        )
                        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
                }
                let encoded = family.encode()?;
                Ok(Some(SimilarityIndexAuditStatus {
                    generation,
                    entries_verified: family.logical_entry_count(),
                    pages_verified,
                    run_hash: *blake3::hash(&encoded).as_bytes(),
                }))
            }
        }
    }

    /// Audits the newest Similarity snapshot and its binding to an Exact Run
    /// Set selected by the caller's Exact-index recovery path.
    ///
    /// # Errors
    ///
    /// Returns audit failures. An unbound or different Exact identity produces
    /// `None`, while the standalone snapshot remains auditable separately.
    pub fn audit_latest_for_exact(
        &self,
        exact_run_set_id: ExactIndexRunSetId,
    ) -> Result<Option<SimilarityIndexAuditStatus>, SimilarityIndexStoreError> {
        let Some(publication) = self.latest_published()? else {
            return Ok(None);
        };
        let LatestSimilarityPublication::Family { name, .. } = &publication else {
            return Ok(None);
        };
        if self.read_family(name)?.source_exact_run_set_id() != Some(exact_run_set_id) {
            return Ok(None);
        }
        self.audit_publication(publication)
    }

    fn latest_published(
        &self,
    ) -> Result<Option<LatestSimilarityPublication>, SimilarityIndexStoreError> {
        let mut latest: Option<LatestSimilarityPublication> = None;
        for name in self.storage.list_names()? {
            let candidate = if let Some(generation) = parse_published_name(&name)? {
                Some(LatestSimilarityPublication::Legacy { generation, name })
            } else {
                parse_family_name(&name)?
                    .map(|generation| LatestSimilarityPublication::Family { generation, name })
            };
            let Some(candidate) = candidate else {
                continue;
            };
            match latest.as_ref() {
                None => latest = Some(candidate),
                Some(current) if candidate.generation() > current.generation() => {
                    latest = Some(candidate);
                }
                Some(current) if candidate.generation() == current.generation() => {
                    return Err(SimilarityIndexStoreError::IdentityMismatch);
                }
                Some(_) => {}
            }
        }
        Ok(latest)
    }

    fn read_envelope(&self, name: &str) -> Result<OpenedSimilarityRun, SimilarityIndexStoreError> {
        let file_length = self.storage.object_len(name)?;
        let block_bytes = u64::try_from(SIMILARITY_INDEX_PAGE_BYTES)
            .expect("ASSERT: Similarity block bytes fit u64");
        if file_length < block_bytes * 3 {
            return Err(SimilarityIndexFormatError::InvalidObjectLength(
                usize::try_from(file_length).unwrap_or(usize::MAX),
            )
            .into());
        }
        let footer_offset = file_length
            .checked_sub(block_bytes)
            .expect("ASSERT: minimum Similarity run length was checked");
        let header = self
            .storage
            .read_exact_at(name, 0, SIMILARITY_INDEX_HEADER_BYTES)?;
        let footer =
            self.storage
                .read_exact_at(name, footer_offset, SIMILARITY_INDEX_HEADER_BYTES)?;
        let descriptor = SimilarityIndexRunDescriptor::decode(&header, &footer, file_length)?;
        Ok(OpenedSimilarityRun {
            descriptor,
            header,
            footer,
            footer_offset,
        })
    }

    fn audit_named(
        &self,
        name: &str,
        mut visit: impl FnMut(SimilarityIndexEntry) -> Result<(), SimilarityIndexStoreError>,
    ) -> Result<SimilarityIndexRunDescriptor, SimilarityIndexStoreError> {
        let envelope = self.read_envelope(name)?;
        let descriptor = envelope.descriptor;
        let mut audit = descriptor.start_hash_audit();
        let mut semantic_entry_page = None;
        audit.update(0, &envelope.header)?;
        for ordinal in 0..descriptor.page_count() {
            let offset = descriptor
                .page_offset(ordinal)
                .expect("ASSERT: validated Similarity page ordinal has an offset");
            let bytes = self
                .storage
                .read_exact_at(name, offset, SIMILARITY_INDEX_PAGE_BYTES)?;
            let page = descriptor.decode_page(ordinal, &bytes)?;
            audit.verify_page(&page)?;
            for entry in page.entries().iter().copied() {
                visit(entry)?;
            }
            audit.update(offset, &bytes)?;
        }
        for ordinal in 0..descriptor.bucket_page_count() {
            let offset = descriptor
                .bucket_page_offset(ordinal)
                .expect("ASSERT: validated Similarity bucket page ordinal has an offset");
            let bytes = self
                .storage
                .read_exact_at(name, offset, SIMILARITY_INDEX_PAGE_BYTES)?;
            let page = descriptor.decode_bucket_page(ordinal, &bytes)?;
            for reference in page.references() {
                let entry = read_entry_at(
                    &self.storage,
                    name,
                    descriptor,
                    reference.entry_ordinal(),
                    &mut semantic_entry_page,
                )?;
                let key = reference.key();
                if entry.fingerprint_profile() != key.fingerprint_profile()
                    || entry.logical_length() != key.logical_length()
                    || entry.superfeatures().get(usize::from(key.slot()))
                        != Some(&key.superfeature())
                {
                    return Err(SimilarityIndexStoreError::IndexCorruption);
                }
            }
            audit.verify_bucket_page(&page)?;
            audit.update(offset, &bytes)?;
        }
        audit.update(envelope.footer_offset, &envelope.footer)?;
        audit.finish()?;
        Ok(descriptor)
    }

    fn audit_partition_named(
        &self,
        name: &str,
    ) -> Result<
        (
            SimilarityIndexRunDescriptor,
            SimilarityBucketKey,
            SimilarityBucketKey,
        ),
        SimilarityIndexStoreError,
    > {
        let descriptor = self.audit_named(name, |_| Ok(()))?;
        let first_offset = descriptor
            .bucket_page_offset(0)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let last_ordinal = descriptor
            .bucket_page_count()
            .checked_sub(1)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let last_offset = descriptor
            .bucket_page_offset(last_ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let first_bytes =
            self.storage
                .read_exact_at(name, first_offset, SIMILARITY_INDEX_PAGE_BYTES)?;
        let first = descriptor.decode_bucket_page(0, &first_bytes)?;
        let last = if last_ordinal == 0 {
            first.clone()
        } else {
            let bytes =
                self.storage
                    .read_exact_at(name, last_offset, SIMILARITY_INDEX_PAGE_BYTES)?;
            descriptor.decode_bucket_page(last_ordinal, &bytes)?
        };
        Ok((descriptor, first.first_key(), last.last_key()))
    }

    fn recover_partition_named(
        &self,
        name: &str,
    ) -> Result<VerifiedSimilarityPartition, SimilarityIndexStoreError> {
        let envelope = self.read_envelope(name)?;
        if let Some(lease) = self
            .storage
            .lease_immutable_file(name, envelope.descriptor.file_length())?
        {
            let mapping = Arc::new(ImmutableSimilarityRun::open(lease, envelope.descriptor)?);
            return Ok(VerifiedSimilarityPartition {
                descriptor: mapping.descriptor(),
                minimum_bucket_key: mapping.minimum_bucket_key(),
                maximum_bucket_key: mapping.maximum_bucket_key(),
                mapping: Some(mapping),
            });
        }
        let (descriptor, minimum_bucket_key, maximum_bucket_key) =
            self.audit_partition_named(name)?;
        Ok(VerifiedSimilarityPartition {
            descriptor,
            minimum_bucket_key,
            maximum_bucket_key,
            mapping: None,
        })
    }

    fn read_family(
        &self,
        name: &str,
    ) -> Result<SimilarityIndexRunFamily, SimilarityIndexStoreError> {
        let length = self.storage.object_len(name)?;
        let length =
            usize::try_from(length).map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
        if length > 16 * 1_024 * 1_024 {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        let bytes = self.storage.read(name)?;
        if bytes.len() != length {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        Ok(SimilarityIndexRunFamily::decode(&bytes)?)
    }
}

struct OpenedSimilarityRun {
    descriptor: SimilarityIndexRunDescriptor,
    header: Vec<u8>,
    footer: Vec<u8>,
    footer_offset: u64,
}

struct VerifiedSimilarityPartition {
    descriptor: SimilarityIndexRunDescriptor,
    minimum_bucket_key: SimilarityBucketKey,
    maximum_bucket_key: SimilarityBucketKey,
    mapping: Option<Arc<ImmutableSimilarityRun>>,
}

pub(crate) struct StagedSimilarityIndex {
    family: SimilarityIndexRunFamily,
    encoded_family: Vec<u8>,
    family_temporary_name: String,
}

impl StagedSimilarityIndex {
    pub(crate) const fn family(&self) -> &SimilarityIndexRunFamily {
        &self.family
    }
}

enum LatestSimilarityPublication {
    Legacy { generation: u64, name: String },
    Family { generation: u64, name: String },
}

impl LatestSimilarityPublication {
    const fn generation(&self) -> u64 {
        match self {
            Self::Legacy { generation, .. } | Self::Family { generation, .. } => *generation,
        }
    }
}

struct TemporarySimilarityFiles<'a, I: StorageIo> {
    storage: &'a I,
    names: Vec<String>,
}

impl<'a, I: StorageIo> TemporarySimilarityFiles<'a, I> {
    fn new(storage: &'a I, names: Vec<String>) -> Self {
        Self { storage, names }
    }
}

impl<I: StorageIo> Drop for TemporarySimilarityFiles<'_, I> {
    fn drop(&mut self) {
        for name in &self.names {
            let _ = remove_if_present(self.storage, name);
        }
    }
}

fn read_entry_at<I: StorageIo>(
    storage: &I,
    name: &str,
    descriptor: SimilarityIndexRunDescriptor,
    entry_ordinal: u32,
    cached_page: &mut Option<(usize, SimilarityIndexPage)>,
) -> Result<SimilarityIndexEntry, SimilarityIndexStoreError> {
    const ENTRIES_PER_PAGE: usize = 25;
    let entry_ordinal =
        usize::try_from(entry_ordinal).map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
    if entry_ordinal >= descriptor.entry_count() {
        return Err(SimilarityIndexStoreError::IndexCorruption);
    }
    let page_ordinal = entry_ordinal / ENTRIES_PER_PAGE;
    if cached_page
        .as_ref()
        .is_none_or(|(cached_ordinal, _)| *cached_ordinal != page_ordinal)
    {
        let offset = descriptor
            .page_offset(page_ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let bytes = storage.read_exact_at(name, offset, SIMILARITY_INDEX_PAGE_BYTES)?;
        let page = descriptor.decode_page(page_ordinal, &bytes)?;
        *cached_page = Some((page_ordinal, page));
    }
    cached_page
        .as_ref()
        .and_then(|(_, page)| page.entries().get(entry_ordinal % ENTRIES_PER_PAGE))
        .copied()
        .ok_or(SimilarityIndexStoreError::IndexCorruption)
}

/// One verified immutable pool index queried directly from fixed 4-KiB pages.
pub struct RecoveredSimilarityIndex<I> {
    storage: I,
    partitions: Box<[RecoveredSimilarityPartition]>,
    status: SimilarityIndexRebuildStatus,
}

struct RecoveredSimilarityPartition {
    name: String,
    descriptor: SimilarityIndexRunDescriptor,
    minimum_bucket_key: SimilarityBucketKey,
    maximum_bucket_key: SimilarityBucketKey,
    mapping: Option<Arc<ImmutableSimilarityRun>>,
    page_cache: Arc<SimilarityPageCache>,
}

impl<I: Clone + StorageIo> RecoveredSimilarityIndex<I> {
    #[must_use]
    pub const fn status(&self) -> SimilarityIndexRebuildStatus {
        self.status
    }

    /// Returns at most 16 pool-wide candidate identities for one target.
    ///
    /// The method hashes and fingerprints `target` once. It never reads Base
    /// payloads and does not claim that a returned Chunk remains live.
    ///
    /// # Errors
    ///
    /// Rejects empty or oversized targets and corrupt entry or bucket pages.
    pub fn candidates(
        &self,
        target: &[u8],
    ) -> Result<Vec<SimilarityBaseCandidate>, SimilarityIndexStoreError> {
        let target_id = ChunkId::of(target);
        self.candidates_prehashed(target_id, target)
    }

    /// Returns candidates while reusing the Chunk identity already computed
    /// by `SeqCDC` ingest.
    ///
    /// The identity is trusted as prior writer work exactly like prehashed
    /// Container publication. Independent readers and scrub still recompute
    /// it from durable bytes.
    ///
    /// # Errors
    ///
    /// Rejects invalid targets, corrupt touched pages or bucket relationships,
    /// unsupported profiles, and bounded allocation failures.
    pub fn candidates_prehashed(
        &self,
        target_id: ChunkId,
        target: &[u8],
    ) -> Result<Vec<SimilarityBaseCandidate>, SimilarityIndexStoreError> {
        let fingerprint = SimilarityFingerprint::v1(target).map_err(map_similarity_error)?;
        let logical_length =
            u32::try_from(target.len()).map_err(|_| SimilarityIndexStoreError::InvalidTarget)?;
        let target_superfeatures = fingerprint.superfeatures();
        let mut cursors: [Option<QueryBucketCursor>; 4] = [None, None, None, None];
        for (slot, superfeature) in target_superfeatures.into_iter().enumerate() {
            let key = SimilarityBucketKey::new(
                fingerprint.profile(),
                u8::try_from(slot).map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
                logical_length,
                superfeature,
            )?;
            let Some(partition_ordinal) = self.partition_for_key(key) else {
                continue;
            };
            let bucket = self.read_bucket(partition_ordinal, key)?;
            if let Some(entry_ordinal) = bucket.get(0) {
                let entry = self.read_entry(partition_ordinal, entry_ordinal)?;
                validate_query_entry(entry, key, slot)?;
                cursors[slot] = Some(QueryBucketCursor {
                    key,
                    partition_ordinal,
                    ordinals: bucket,
                    next: 1,
                    current: entry,
                });
            }
        }

        let mut candidates = Vec::with_capacity(MAX_SIMILARITY_CANDIDATES);
        while let Some(chunk_id) = cursors
            .iter()
            .flatten()
            .map(|cursor| cursor.current.chunk_id())
            .min()
        {
            let mut selected = None;
            let mut matched_slots = 0_u8;
            for (slot, cursor) in cursors.iter_mut().enumerate() {
                let Some(current) = cursor.as_ref().map(|cursor| cursor.current) else {
                    continue;
                };
                if current.chunk_id() != chunk_id {
                    continue;
                }
                if selected.is_some_and(|previous| previous != current) {
                    return Err(SimilarityIndexStoreError::IndexCorruption);
                }
                selected = Some(current);
                matched_slots |= 1_u8 << slot;
                let active = cursor
                    .as_mut()
                    .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
                if let Some(ordinal) = active.ordinals.get(active.next) {
                    active.next = active
                        .next
                        .checked_add(1)
                        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
                    let next = self.read_entry(active.partition_ordinal, ordinal)?;
                    validate_query_entry(next, active.key, slot)?;
                    if next.chunk_id() <= current.chunk_id() {
                        return Err(SimilarityIndexStoreError::IndexCorruption);
                    }
                    active.current = next;
                } else {
                    *cursor = None;
                }
            }
            let entry = selected.ok_or(SimilarityIndexStoreError::IndexCorruption)?;
            if entry.chunk_id() == target_id {
                continue;
            }
            if entry.logical_length() != logical_length
                || entry.fingerprint_profile() != fingerprint.profile()
                || matched_slots == 0
                || entry
                    .superfeatures()
                    .into_iter()
                    .zip(target_superfeatures)
                    .enumerate()
                    .any(|(slot, (candidate, target))| {
                        matched_slots & (1_u8 << slot) != 0 && candidate != target
                    })
            {
                return Err(SimilarityIndexStoreError::IndexCorruption);
            }
            let candidate = SimilarityBaseCandidate {
                chunk_id: entry.chunk_id(),
                logical_length,
                sketch_distance: fingerprint
                    .distance_from_sketch(entry.fingerprint_profile(), entry.sketch())
                    .map_err(map_similarity_error)?,
            };
            insert_ranked_candidate(&mut candidates, candidate);
        }
        Ok(candidates)
    }

    fn partition_for_key(&self, key: SimilarityBucketKey) -> Option<usize> {
        let ordinal = self
            .partitions
            .partition_point(|partition| partition.maximum_bucket_key < key);
        self.partitions
            .get(ordinal)
            .filter(|partition| partition.minimum_bucket_key <= key)
            .map(|_| ordinal)
    }

    fn read_bucket(
        &self,
        partition_ordinal: usize,
        key: SimilarityBucketKey,
    ) -> Result<BucketOrdinals, SimilarityIndexStoreError> {
        let partition = self
            .partitions
            .get(partition_ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let mut lower = 0_usize;
        let mut upper = partition.descriptor.bucket_page_count();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let page = self.read_bucket_page(partition_ordinal, middle)?;
            if page.last_key() < key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }

        let mut ordinals = BucketOrdinals::default();
        for ordinal in lower..partition.descriptor.bucket_page_count() {
            let page = self.read_bucket_page(partition_ordinal, ordinal)?;
            if page.first_key() > key {
                break;
            }
            for reference in page.references() {
                if reference.key() == key {
                    ordinals.push(reference.entry_ordinal())?;
                }
            }
            if page.last_key() > key {
                break;
            }
        }
        Ok(ordinals)
    }

    fn read_bucket_page(
        &self,
        partition_ordinal: usize,
        ordinal: usize,
    ) -> Result<Arc<SimilarityBucketPage>, SimilarityIndexStoreError> {
        let partition = self
            .partitions
            .get(partition_ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        if let Some(page) = partition.page_cache.bucket_pages.get(ordinal) {
            return Ok(page);
        }
        let offset = partition
            .descriptor
            .bucket_page_offset(ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let bytes = partition.read_page(&self.storage, offset)?;
        let page = Arc::new(
            partition
                .descriptor
                .decode_bucket_page(ordinal, bytes.as_ref())?,
        );
        partition
            .page_cache
            .bucket_pages
            .insert(ordinal, Arc::clone(&page));
        Ok(page)
    }

    fn read_entry(
        &self,
        partition_ordinal: usize,
        entry_ordinal: u32,
    ) -> Result<SimilarityIndexEntry, SimilarityIndexStoreError> {
        const ENTRIES_PER_PAGE: usize = 25;
        let partition = self
            .partitions
            .get(partition_ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let entry_ordinal = usize::try_from(entry_ordinal)
            .map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
        if entry_ordinal >= partition.descriptor.entry_count() {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        let page_ordinal = entry_ordinal / ENTRIES_PER_PAGE;
        let page = if let Some(page) = partition.page_cache.entry_pages.get(page_ordinal) {
            page
        } else {
            let offset = partition
                .descriptor
                .page_offset(page_ordinal)
                .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
            let bytes = partition.read_page(&self.storage, offset)?;
            let page = Arc::new(
                partition
                    .descriptor
                    .decode_page(page_ordinal, bytes.as_ref())?,
            );
            partition
                .page_cache
                .entry_pages
                .insert(page_ordinal, Arc::clone(&page));
            page
        };
        page.entries()
            .get(entry_ordinal % ENTRIES_PER_PAGE)
            .copied()
            .ok_or(SimilarityIndexStoreError::IndexCorruption)
    }
}

impl RecoveredSimilarityPartition {
    fn read_page<'a, I: StorageIo>(
        &'a self,
        storage: &I,
        offset: u64,
    ) -> Result<SimilarityPageBytes<'a>, SimilarityIndexStoreError> {
        match &self.mapping {
            Some(mapping) => Ok(SimilarityPageBytes::Borrowed(mapping.page(offset)?)),
            None => Ok(SimilarityPageBytes::Owned(storage.read_exact_at(
                &self.name,
                offset,
                SIMILARITY_INDEX_PAGE_BYTES,
            )?)),
        }
    }
}

enum SimilarityPageBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl AsRef<[u8]> for SimilarityPageBytes<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }
}

const SIMILARITY_PAGE_CACHE_SLOTS: usize = 256;

#[repr(align(64))]
struct SimilarityPageCacheSlot<P>(Mutex<Option<(usize, Arc<P>)>>);

struct DirectSimilarityPageCache<P> {
    slots: Box<[SimilarityPageCacheSlot<P>]>,
}

impl<P> DirectSimilarityPageCache<P> {
    fn new() -> Self {
        assert!(
            SIMILARITY_PAGE_CACHE_SLOTS.is_power_of_two(),
            "ASSERT: Similarity page-cache slots use a power-of-two mask"
        );
        let slots = std::iter::repeat_with(|| SimilarityPageCacheSlot(Mutex::new(None)))
            .take(SIMILARITY_PAGE_CACHE_SLOTS)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }

    fn get(&self, ordinal: usize) -> Option<Arc<P>> {
        let slot = &self.slots[ordinal & (self.slots.len() - 1)];
        let guard = slot
            .0
            .lock()
            .expect("ASSERT: Similarity page-cache slot lock poisoned");
        guard.as_ref().and_then(|(cached_ordinal, page)| {
            (*cached_ordinal == ordinal).then(|| Arc::clone(page))
        })
    }

    fn insert(&self, ordinal: usize, page: Arc<P>) {
        let slot = &self.slots[ordinal & (self.slots.len() - 1)];
        *slot
            .0
            .lock()
            .expect("ASSERT: Similarity page-cache slot lock poisoned") = Some((ordinal, page));
    }
}

struct SimilarityPageCache {
    entry_pages: DirectSimilarityPageCache<SimilarityIndexPage>,
    bucket_pages: DirectSimilarityPageCache<SimilarityBucketPage>,
}

impl SimilarityPageCache {
    fn new() -> Self {
        Self {
            entry_pages: DirectSimilarityPageCache::new(),
            bucket_pages: DirectSimilarityPageCache::new(),
        }
    }
}

const _: () = assert!(std::mem::align_of::<SimilarityPageCacheSlot<SimilarityIndexPage>>() == 64);

#[derive(Clone, Copy)]
struct BucketOrdinals {
    values: [u32; 64],
    len: u8,
}

impl Default for BucketOrdinals {
    fn default() -> Self {
        Self {
            values: [0; 64],
            len: 0,
        }
    }
}

impl BucketOrdinals {
    fn push(&mut self, ordinal: u32) -> Result<(), SimilarityIndexStoreError> {
        let index = usize::from(self.len);
        if index >= self.values.len() || index != 0 && self.values[index - 1] >= ordinal {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        self.values[index] = ordinal;
        self.len = self
            .len
            .checked_add(1)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        Ok(())
    }

    fn get(self, index: usize) -> Option<u32> {
        (index < usize::from(self.len)).then_some(self.values[index])
    }
}

#[derive(Clone, Copy)]
struct QueryBucketCursor {
    key: SimilarityBucketKey,
    partition_ordinal: usize,
    ordinals: BucketOrdinals,
    next: usize,
    current: SimilarityIndexEntry,
}

fn validate_query_entry(
    entry: SimilarityIndexEntry,
    key: SimilarityBucketKey,
    slot: usize,
) -> Result<(), SimilarityIndexStoreError> {
    if entry.fingerprint_profile() != key.fingerprint_profile()
        || entry.logical_length() != key.logical_length()
        || entry.superfeatures().get(slot) != Some(&key.superfeature())
    {
        return Err(SimilarityIndexStoreError::IndexCorruption);
    }
    Ok(())
}

fn insert_ranked_candidate(
    candidates: &mut Vec<SimilarityBaseCandidate>,
    candidate: SimilarityBaseCandidate,
) {
    let key = (candidate.sketch_distance, candidate.chunk_id);
    let position =
        candidates.partition_point(|existing| (existing.sketch_distance, existing.chunk_id) < key);
    if candidates.len() < MAX_SIMILARITY_CANDIDATES {
        candidates.insert(position, candidate);
    } else if position < MAX_SIMILARITY_CANDIDATES {
        candidates.pop();
        candidates.insert(position, candidate);
    }
}

/// One pool-wide candidate identity awaiting Exact Index resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimilarityBaseCandidate {
    chunk_id: ChunkId,
    logical_length: u32,
    sketch_distance: u16,
}

impl SimilarityBaseCandidate {
    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }

    #[must_use]
    pub const fn sketch_distance(self) -> u16 {
        self.sketch_distance
    }
}

/// Evidence from one complete streamed snapshot rebuild.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimilarityIndexRebuildStatus {
    generation: u64,
    entries_streamed: u64,
    resident_representatives: u64,
    buckets: u64,
    read_mode: SimilarityIndexReadMode,
    source_exact_run_set_id: Option<ExactIndexRunSetId>,
}

/// Physical page source selected for a recovered Similarity snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimilarityIndexReadMode {
    /// Bounded positional reads through the generic storage adapter.
    ReadExactAt,
    /// Fully audited read-only mappings protected by generation leases.
    Mmap,
}

impl SimilarityIndexRebuildStatus {
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn entries_streamed(self) -> u64 {
        self.entries_streamed
    }

    #[must_use]
    pub const fn resident_representatives(self) -> u64 {
        self.resident_representatives
    }

    #[must_use]
    pub const fn buckets(self) -> u64 {
        self.buckets
    }

    #[must_use]
    pub const fn read_mode(self) -> SimilarityIndexReadMode {
        self.read_mode
    }

    /// Exact Run Set built from the same verified pool scan, when the durable
    /// family uses the paired-rebuild format.
    #[must_use]
    pub const fn source_exact_run_set_id(self) -> Option<ExactIndexRunSetId> {
        self.source_exact_run_set_id
    }
}

/// Payload-free evidence from one complete offline Similarity Run audit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimilarityIndexAuditStatus {
    generation: u64,
    entries_verified: u64,
    pages_verified: u64,
    run_hash: [u8; 32],
}

impl SimilarityIndexAuditStatus {
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn entries_verified(self) -> u64 {
        self.entries_verified
    }

    #[must_use]
    pub const fn pages_verified(self) -> u64 {
        self.pages_verified
    }

    #[must_use]
    pub const fn run_hash(self) -> [u8; 32] {
        self.run_hash
    }
}

/// Computes the canonical v1 durable entry for verified logical bytes.
///
/// # Errors
///
/// Rejects empty or oversized logical chunks and arithmetic or format errors.
pub fn similarity_index_entry_v1(
    bytes: &[u8],
) -> Result<SimilarityIndexEntry, SimilarityIndexStoreError> {
    similarity_index_entry_v1_from_verified(ChunkId::of(bytes), bytes)
}

pub(crate) fn similarity_index_entry_v1_from_verified(
    chunk_id: ChunkId,
    bytes: &[u8],
) -> Result<SimilarityIndexEntry, SimilarityIndexStoreError> {
    let fingerprint = SimilarityFingerprint::v1(bytes).map_err(map_similarity_error)?;
    let logical_length =
        u32::try_from(bytes.len()).map_err(|_| SimilarityIndexStoreError::InvalidTarget)?;
    Ok(SimilarityIndexEntry::new(
        chunk_id,
        logical_length,
        fingerprint.profile(),
        fingerprint.superfeatures(),
        fingerprint.sketch(),
    )?)
}

fn stream_similarity_run(
    run: &SimilarityIndexRun,
    mut visit: impl FnMut(u64, &[u8]) -> Result<(), SimilarityIndexStoreError>,
) -> Result<SimilarityIndexRunDescriptor, SimilarityIndexStoreError> {
    let mut encoder = SimilarityIndexRunStreamEncoder::new(run.stream_layout())?;
    let mut offset = 0_u64;
    let page_bytes = u64::try_from(SIMILARITY_INDEX_PAGE_BYTES)
        .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
    visit(offset, encoder.header())?;
    offset = offset
        .checked_add(page_bytes)
        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    for entries in run.entries().chunks(SIMILARITY_INDEX_ENTRIES_PER_PAGE) {
        let page = encoder.encode_next_entry_page(entries)?;
        visit(offset, &page)?;
        offset = offset
            .checked_add(page_bytes)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    }
    for references in run
        .bucket_references()
        .chunks(SIMILARITY_BUCKET_REFERENCES_PER_PAGE)
    {
        let page = encoder.encode_next_bucket_page(references)?;
        visit(offset, &page)?;
        offset = offset
            .checked_add(page_bytes)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    }
    let (footer, descriptor) = encoder.finish()?;
    visit(offset, &footer)?;
    let observed_length = offset
        .checked_add(page_bytes)
        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    if observed_length != descriptor.file_length() {
        return Err(SimilarityIndexStoreError::IdentityMismatch);
    }
    Ok(descriptor)
}

fn verify_expected_descriptor(
    expected: SimilarityIndexRunDescriptor,
    observed: SimilarityIndexRunDescriptor,
) -> Result<(), SimilarityIndexStoreError> {
    if expected.fingerprint_profile() != observed.fingerprint_profile()
        || expected.bucket_profile() != observed.bucket_profile()
        || expected.generation() != observed.generation()
        || expected.file_length() != observed.file_length()
        || expected.run_hash() != observed.run_hash()
    {
        return Err(SimilarityIndexStoreError::PublishVerificationMismatch);
    }
    Ok(())
}

fn verify_partition_reference(
    reference: SimilarityIndexPartitionRef,
    descriptor: SimilarityIndexRunDescriptor,
    minimum_bucket_key: SimilarityBucketKey,
    maximum_bucket_key: SimilarityBucketKey,
) -> Result<(), SimilarityIndexStoreError> {
    if reference.run_hash() != descriptor.run_hash()
        || reference.file_length() != descriptor.file_length()
        || usize::try_from(reference.entry_count()).ok() != Some(descriptor.entry_count())
        || usize::try_from(reference.bucket_count()).ok() != Some(descriptor.bucket_count())
        || usize::try_from(reference.bucket_reference_count()).ok()
            != Some(descriptor.bucket_reference_count())
        || reference.minimum_chunk_id() != descriptor.minimum_chunk_id()
        || reference.maximum_chunk_id() != descriptor.maximum_chunk_id()
        || reference.minimum_bucket_key() != minimum_bucket_key
        || reference.maximum_bucket_key() != maximum_bucket_key
    {
        return Err(SimilarityIndexStoreError::PublishVerificationMismatch);
    }
    Ok(())
}

fn require_v1_profiles(
    fingerprint_profile: u16,
    bucket_profile: u16,
) -> Result<(), SimilarityIndexStoreError> {
    if fingerprint_profile != SIMILARITY_PROFILE_V1
        || bucket_profile != SIMILARITY_BUCKET_PROFILE_V1
    {
        return Err(SimilarityIndexStoreError::UnsupportedProfile);
    }
    Ok(())
}

fn remove_if_present<I: StorageIo>(storage: &I, name: &str) -> io::Result<()> {
    match storage.remove_file(name) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn ensure_object<I: StorageIo>(storage: &I, name: &str) -> io::Result<()> {
    match storage.create_new(name) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

fn published_name(generation: u64) -> String {
    format!(
        "similarity.{SIMILARITY_PROFILE_V1:04x}.{SIMILARITY_BUCKET_PROFILE_V1:04x}.{generation:016x}.fds"
    )
}

fn family_name(generation: u64) -> String {
    format!(
        "similarity-family.{SIMILARITY_PROFILE_V1:04x}.{SIMILARITY_BUCKET_PROFILE_V1:04x}.{generation:016x}.fdsf"
    )
}

fn partition_name(generation: u64, partition_ordinal: u16) -> String {
    format!(
        "similarity-part.{SIMILARITY_PROFILE_V1:04x}.{SIMILARITY_BUCKET_PROFILE_V1:04x}.{generation:016x}.{partition_ordinal:04x}.fds"
    )
}

fn parse_published_name(name: &str) -> Result<Option<u64>, SimilarityIndexStoreError> {
    if !name.starts_with("similarity.") || name.strip_suffix(".fds").is_none() {
        return Ok(None);
    }
    let fields = name.split('.').collect::<Vec<_>>();
    if fields.len() != 5
        || fields[0] != "similarity"
        || fields[1] != format!("{SIMILARITY_PROFILE_V1:04x}")
        || fields[2] != format!("{SIMILARITY_BUCKET_PROFILE_V1:04x}")
        || fields[4] != "fds"
    {
        return Err(SimilarityIndexStoreError::IdentityMismatch);
    }
    let generation = u64::from_str_radix(fields[3], 16)
        .map_err(|_| SimilarityIndexStoreError::IdentityMismatch)?;
    if generation == 0 || fields[3].len() != 16 {
        return Err(SimilarityIndexStoreError::IdentityMismatch);
    }
    Ok(Some(generation))
}

fn parse_family_name(name: &str) -> Result<Option<u64>, SimilarityIndexStoreError> {
    if !name.starts_with("similarity-family.") || name.strip_suffix(".fdsf").is_none() {
        return Ok(None);
    }
    let fields = name.split('.').collect::<Vec<_>>();
    if fields.len() != 5
        || fields[0] != "similarity-family"
        || fields[1] != format!("{SIMILARITY_PROFILE_V1:04x}")
        || fields[2] != format!("{SIMILARITY_BUCKET_PROFILE_V1:04x}")
        || fields[4] != "fdsf"
    {
        return Err(SimilarityIndexStoreError::IdentityMismatch);
    }
    let generation = u64::from_str_radix(fields[3], 16)
        .map_err(|_| SimilarityIndexStoreError::IdentityMismatch)?;
    if generation == 0 || fields[3].len() != 16 {
        return Err(SimilarityIndexStoreError::IdentityMismatch);
    }
    Ok(Some(generation))
}

fn parse_partition_name(name: &str) -> Result<Option<u64>, SimilarityIndexStoreError> {
    if !name.starts_with("similarity-part.") || name.strip_suffix(".fds").is_none() {
        return Ok(None);
    }
    let fields = name.split('.').collect::<Vec<_>>();
    if fields.len() != 6
        || fields[0] != "similarity-part"
        || fields[1] != format!("{SIMILARITY_PROFILE_V1:04x}")
        || fields[2] != format!("{SIMILARITY_BUCKET_PROFILE_V1:04x}")
        || fields[5] != "fds"
        || fields[3].len() != 16
        || fields[4].len() != 4
    {
        return Err(SimilarityIndexStoreError::IdentityMismatch);
    }
    let generation = u64::from_str_radix(fields[3], 16)
        .map_err(|_| SimilarityIndexStoreError::IdentityMismatch)?;
    let ordinal = u16::from_str_radix(fields[4], 16)
        .map_err(|_| SimilarityIndexStoreError::IdentityMismatch)?;
    if generation == 0 || partition_name(generation, ordinal) != name {
        return Err(SimilarityIndexStoreError::IdentityMismatch);
    }
    Ok(Some(generation))
}

fn map_similarity_error(error: SimilarityError) -> SimilarityIndexStoreError {
    match error {
        SimilarityError::EmptyChunk
        | SimilarityError::ChunkTooLarge
        | SimilarityError::InvalidLogicalLength
        | SimilarityError::ProfileMismatch
        | SimilarityError::CandidateLimitExceeded => SimilarityIndexStoreError::InvalidTarget,
        SimilarityError::ArithmeticOverflow => SimilarityIndexStoreError::CounterOverflow,
        _ => SimilarityIndexStoreError::IndexCorruption,
    }
}

#[derive(Debug)]
pub enum SimilarityIndexStoreError {
    Io(io::Error),
    Format(SimilarityIndexFormatError),
    UnsupportedProfile,
    IdentityMismatch,
    PublishVerificationMismatch,
    InvalidTarget,
    IndexCorruption,
    CounterOverflow,
    OutOfMemory,
    TooManyPartitions,
}

impl fmt::Display for SimilarityIndexStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for SimilarityIndexStoreError {}

impl From<io::Error> for SimilarityIndexStoreError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SimilarityIndexFormatError> for SimilarityIndexStoreError {
    fn from(error: SimilarityIndexFormatError) -> Self {
        Self::Format(error)
    }
}

impl From<SimilarityIndexFamilyError> for SimilarityIndexStoreError {
    fn from(error: SimilarityIndexFamilyError) -> Self {
        match error {
            SimilarityIndexFamilyError::OutOfMemory => Self::OutOfMemory,
            SimilarityIndexFamilyError::TooManyPartitions => Self::TooManyPartitions,
            SimilarityIndexFamilyError::ArithmeticOverflow => Self::CounterOverflow,
            _ => Self::IndexCorruption,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_stream_emits_only_complete_format_pages() {
        let entries = (0_u64..400)
            .map(|ordinal| {
                SimilarityIndexEntry::new(
                    ChunkId::of(&ordinal.to_le_bytes()),
                    64 * 1_024,
                    SIMILARITY_PROFILE_V1,
                    [ordinal, ordinal + 1, ordinal + 2, ordinal + 3],
                    [ordinal.rotate_left(7); 8],
                )
                .expect("fixture entry is valid")
            })
            .collect();
        let run = SimilarityIndexRun::new(
            SIMILARITY_PROFILE_V1,
            SIMILARITY_BUCKET_PROFILE_V1,
            91,
            entries,
        )
        .expect("construct streaming fixture");
        let mut ranges = Vec::new();
        let descriptor = stream_similarity_run(&run, |offset, bytes| {
            ranges.push((offset, bytes.len()));
            Ok(())
        })
        .expect("stream fixture");

        assert!(
            ranges
                .iter()
                .all(|(_, length)| *length == SIMILARITY_INDEX_PAGE_BYTES)
        );
        assert!(
            ranges
                .windows(2)
                .all(|pair| pair[1].0 == pair[0].0 + SIMILARITY_INDEX_PAGE_BYTES as u64)
        );
        assert_eq!(
            ranges.len() * SIMILARITY_INDEX_PAGE_BYTES,
            usize::try_from(descriptor.file_length()).expect("fixture length fits usize")
        );
    }
}

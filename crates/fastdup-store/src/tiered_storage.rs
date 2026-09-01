use std::collections::BTreeSet;
use std::fmt;
use std::io;

use fastdup_format::VerifiedContainerPublication;

use crate::{
    ContainerPlacement, ImmutableFileLease, OwnedContainerPublication, StorageIo, StoreError,
};

/// One physical tier selected for an already existing immutable object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingTier {
    Data,
    SmallFile,
}

/// A cross-tier namespace violation detected before an object is exposed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TieredStorageError {
    name: String,
}

impl fmt::Display for TieredStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "immutable storage object {:?} exists in both physical tiers",
            self.name
        )
    }
}

impl std::error::Error for TieredStorageError {}

/// One logical immutable-object namespace backed by DATA and Small-File tiers.
///
/// Publication consumes the writer's [`ContainerPlacement`] exactly once.
/// Every later operation resolves the globally unique name without exposing a
/// tier bit to Container Locations, Exact acceleration, recovery, scrub, or GC.
/// A duplicate name across tiers fails closed.
#[derive(Clone, Debug)]
pub struct TieredStorageIo<D, S> {
    data: D,
    small_file: S,
}

impl<D, S> TieredStorageIo<D, S> {
    #[must_use]
    pub const fn new(data: D, small_file: S) -> Self {
        Self { data, small_file }
    }

    #[must_use]
    pub const fn data(&self) -> &D {
        &self.data
    }

    #[must_use]
    pub const fn small_file(&self) -> &S {
        &self.small_file
    }
}

impl<D: StorageIo, S: StorageIo> TieredStorageIo<D, S> {
    fn locate(&self, name: &str) -> io::Result<Option<ExistingTier>> {
        let in_data = self.data.exists(name)?;
        let in_small_file = self.small_file.exists(name)?;
        match (in_data, in_small_file) {
            (true, false) => Ok(Some(ExistingTier::Data)),
            (false, true) => Ok(Some(ExistingTier::SmallFile)),
            (false, false) => Ok(None),
            (true, true) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                TieredStorageError {
                    name: name.to_owned(),
                },
            )),
        }
    }

    fn existing(&self, name: &str) -> io::Result<ExistingTier> {
        self.locate(name)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("immutable storage object {name:?} does not exist"),
            )
        })
    }

    fn storage(&self, tier: ExistingTier) -> &dyn StorageIo {
        match tier {
            ExistingTier::Data => &self.data,
            ExistingTier::SmallFile => &self.small_file,
        }
    }
}

impl<D, S> StorageIo for TieredStorageIo<D, S>
where
    D: StorageIo,
    S: StorageIo,
{
    fn create_new(&self, name: &str) -> io::Result<()> {
        if self.locate(name)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "immutable storage object already exists",
            ));
        }
        // Primitive resumable writers are maintenance writers and deliberately
        // default to DATA. Policy-selected frontend publication uses the owned
        // publication method below.
        self.data.create_new(name)
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        Ok(self.locate(name)?.is_some())
    }

    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.storage(self.existing(name)?)
            .write_at(name, offset, bytes)
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        self.storage(self.existing(name)?).read(name)
    }

    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.storage(self.existing(name)?).object_len(name)
    }

    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.storage(self.existing(name)?)
            .read_exact_at(name, offset, length)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        let mut names = BTreeSet::new();
        for name in self.data.list_names()? {
            names.insert(name);
        }
        for name in self.small_file.list_names()? {
            if !names.insert(name.clone()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    TieredStorageError { name },
                ));
            }
        }
        Ok(names.into_iter().collect())
    }

    fn visit_names(&self, visitor: &mut dyn FnMut(&str)) -> io::Result<()> {
        for name in self.list_names()? {
            visitor(&name);
        }
        Ok(())
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.storage(self.existing(name)?).set_len(name, length)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        self.storage(self.existing(name)?).sync_file(name)
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        if self.locate(published_name)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "published immutable storage object already exists",
            ));
        }
        self.storage(self.existing(temporary_name)?)
            .publish_noreplace(temporary_name, published_name)
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.storage(self.existing(name)?).remove_file(name)
    }

    fn sync_root(&self) -> io::Result<()> {
        self.data.sync_root()?;
        self.small_file.sync_root()
    }

    fn lease_immutable_file(
        &self,
        name: &str,
        expected_length: u64,
    ) -> io::Result<Option<ImmutableFileLease>> {
        self.storage(self.existing(name)?)
            .lease_immutable_file(name, expected_length)
    }

    fn publish_owned_container(
        &self,
        publication: OwnedContainerPublication,
    ) -> Result<VerifiedContainerPublication, StoreError> {
        let published_name = publication.published_name().to_owned();
        if self.locate(&published_name)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "published Container exists in one physical tier",
            )
            .into());
        }
        match publication.placement() {
            ContainerPlacement::Data => self.data.publish_owned_container(publication),
            ContainerPlacement::SmallFile => self.small_file.publish_owned_container(publication),
        }
    }
}

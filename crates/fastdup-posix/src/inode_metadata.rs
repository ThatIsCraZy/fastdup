use std::collections::BTreeMap;

use crate::{FileKind, PosixError};

pub const FS_IMMUTABLE_FL: u32 = 0x0000_0010;
pub const POSIX_ACL_ACCESS_XATTR: &[u8] = b"system.posix_acl_access";
pub const POSIX_ACL_DEFAULT_XATTR: &[u8] = b"system.posix_acl_default";

const SUPPORTED_FILE_FLAGS: u32 = FS_IMMUTABLE_FL;
const MAXIMUM_XATTR_NAME_BYTES: usize = 255;
const MAXIMUM_XATTR_VALUE_BYTES: usize = 65_536;
const MAXIMUM_XATTRS_PER_INODE: usize = 1_024;
const MAXIMUM_XATTR_BYTES_PER_INODE: usize = 1_048_576;
const POSIX_ACL_XATTR_VERSION: u32 = 2;
const ACL_USER_OBJ: u16 = 0x01;
const ACL_USER: u16 = 0x02;
const ACL_GROUP_OBJ: u16 = 0x04;
const ACL_GROUP: u16 = 0x08;
const ACL_MASK: u16 = 0x10;
const ACL_OTHER: u16 = 0x20;
const ACL_UNDEFINED_ID: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XattrSetMode {
    Upsert,
    Create,
    Replace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtendedAttribute {
    name: Vec<u8>,
    value: Vec<u8>,
}

impl ExtendedAttribute {
    /// Creates one byte-exact extended attribute.
    ///
    /// # Errors
    ///
    /// Rejects invalid names, unsupported namespaces, oversized values, and
    /// malformed POSIX ACL wire values.
    pub fn new(kind: FileKind, name: Vec<u8>, value: Vec<u8>) -> Result<Self, PosixError> {
        validate_name(&name)?;
        validate_value(kind, &name, &value)?;
        Ok(Self { name, value })
    }

    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InodeMetadata {
    file_flags: u32,
    xattrs: BTreeMap<Vec<u8>, Vec<u8>>,
    xattr_bytes: usize,
}

impl InodeMetadata {
    pub(crate) fn for_child(
        &self,
        kind: FileKind,
        requested_mode: u16,
        umask: u16,
    ) -> Result<(u16, Self), PosixError> {
        let Some(default_acl) = self.xattrs.get(POSIX_ACL_DEFAULT_XATTR) else {
            return Ok((requested_mode & !umask & 0o7777, Self::default()));
        };
        let access_acl = masked_acl(default_acl, requested_mode)?;
        let mode = requested_mode & !0o777 | access_acl_mode(&access_acl)?;
        let mut metadata = Self::default();
        metadata.set_xattr(
            kind,
            POSIX_ACL_ACCESS_XATTR,
            &access_acl,
            XattrSetMode::Create,
        )?;
        if kind == FileKind::Directory {
            metadata.set_xattr(
                kind,
                POSIX_ACL_DEFAULT_XATTR,
                default_acl,
                XattrSetMode::Create,
            )?;
        }
        Ok((mode & 0o7777, metadata))
    }

    /// Reconstructs one verified metadata image.
    ///
    /// # Errors
    ///
    /// Rejects unsupported file flags, duplicate attributes, invalid ACLs,
    /// and per-inode metadata beyond the configured bound.
    pub fn new(
        kind: FileKind,
        file_flags: u32,
        xattrs: Vec<ExtendedAttribute>,
    ) -> Result<Self, PosixError> {
        validate_file_flags(file_flags)?;
        let mut metadata = Self {
            file_flags,
            ..Self::default()
        };
        for xattr in xattrs {
            let name = xattr.name;
            let value = xattr.value;
            validate_value(kind, &name, &value)?;
            let added = name
                .len()
                .checked_add(value.len())
                .ok_or(PosixError::TooBig)?;
            metadata.xattr_bytes = metadata
                .xattr_bytes
                .checked_add(added)
                .ok_or(PosixError::TooBig)?;
            if metadata.xattr_bytes > MAXIMUM_XATTR_BYTES_PER_INODE
                || metadata.xattrs.insert(name, value).is_some()
            {
                return Err(PosixError::InvalidArgument);
            }
        }
        if metadata.xattrs.len() > MAXIMUM_XATTRS_PER_INODE {
            return Err(PosixError::TooBig);
        }
        Ok(metadata)
    }

    #[must_use]
    pub const fn file_flags(&self) -> u32 {
        self.file_flags
    }

    #[must_use]
    pub const fn is_immutable(&self) -> bool {
        self.file_flags & FS_IMMUTABLE_FL != 0
    }

    #[must_use]
    pub fn xattrs(&self) -> impl ExactSizeIterator<Item = ExtendedAttribute> + '_ {
        self.xattrs.iter().map(|(name, value)| ExtendedAttribute {
            name: name.clone(),
            value: value.clone(),
        })
    }

    pub(crate) fn get_xattr(&self, name: &[u8]) -> Result<Vec<u8>, PosixError> {
        validate_name(name)?;
        self.xattrs.get(name).cloned().ok_or(PosixError::NoData)
    }

    pub(crate) fn list_xattrs(&self) -> Result<Vec<u8>, PosixError> {
        let length = self.xattrs.keys().try_fold(0_usize, |total, name| {
            total
                .checked_add(name.len())
                .and_then(|value| value.checked_add(1))
                .ok_or(PosixError::TooBig)
        })?;
        let mut list = Vec::new();
        list.try_reserve_exact(length)
            .map_err(|_| PosixError::OutOfMemory)?;
        for name in self.xattrs.keys() {
            list.extend_from_slice(name);
            list.push(0);
        }
        Ok(list)
    }

    pub(crate) fn set_xattr(
        &mut self,
        kind: FileKind,
        name: &[u8],
        value: &[u8],
        mode: XattrSetMode,
    ) -> Result<Option<u16>, PosixError> {
        validate_name(name)?;
        validate_value(kind, name, value)?;
        let present = self.xattrs.contains_key(name);
        match (mode, present) {
            (XattrSetMode::Create, true) => return Err(PosixError::Exists),
            (XattrSetMode::Replace, false) => return Err(PosixError::NoData),
            _ => {}
        }
        if !present && self.xattrs.len() == MAXIMUM_XATTRS_PER_INODE {
            return Err(PosixError::TooBig);
        }
        let previous = self.xattrs.get(name).map_or(0, |stored| {
            name.len()
                .checked_add(stored.len())
                .expect("ASSERT: stored xattr accounting fits usize")
        });
        let replacement = name
            .len()
            .checked_add(value.len())
            .ok_or(PosixError::TooBig)?;
        let next_bytes = self
            .xattr_bytes
            .checked_sub(previous)
            .and_then(|bytes| bytes.checked_add(replacement))
            .ok_or(PosixError::TooBig)?;
        if next_bytes > MAXIMUM_XATTR_BYTES_PER_INODE {
            return Err(PosixError::TooBig);
        }
        let mut owned_value = Vec::new();
        owned_value
            .try_reserve_exact(value.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        owned_value.extend_from_slice(value);
        let mut owned_name = Vec::new();
        owned_name
            .try_reserve_exact(name.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        owned_name.extend_from_slice(name);
        self.xattrs.insert(owned_name, owned_value);
        self.xattr_bytes = next_bytes;
        if name == POSIX_ACL_ACCESS_XATTR {
            Ok(Some(access_acl_mode(value)?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn remove_xattr(&mut self, name: &[u8]) -> Result<(), PosixError> {
        validate_name(name)?;
        let value = self.xattrs.remove(name).ok_or(PosixError::NoData)?;
        self.xattr_bytes = self
            .xattr_bytes
            .checked_sub(
                name.len()
                    .checked_add(value.len())
                    .expect("ASSERT: stored xattr accounting fits usize"),
            )
            .expect("ASSERT: removed xattr bytes were accounted");
        Ok(())
    }

    pub(crate) fn set_file_flags(&mut self, flags: u32) -> Result<(), PosixError> {
        validate_file_flags(flags)?;
        self.file_flags = flags;
        Ok(())
    }

    pub(crate) fn chmod(&mut self, requested_mode: u16) -> Result<u16, PosixError> {
        let Some(access_acl) = self.xattrs.get_mut(POSIX_ACL_ACCESS_XATTR) else {
            return Ok(requested_mode & 0o7777);
        };
        rewrite_acl_mode(access_acl, requested_mode)?;
        Ok(requested_mode & 0o7777)
    }
}

fn validate_file_flags(flags: u32) -> Result<(), PosixError> {
    if flags & !SUPPORTED_FILE_FLAGS != 0 {
        return Err(PosixError::Unsupported);
    }
    Ok(())
}

fn validate_name(name: &[u8]) -> Result<(), PosixError> {
    if name.is_empty() || name.contains(&0) {
        return Err(PosixError::InvalidArgument);
    }
    if name.len() > MAXIMUM_XATTR_NAME_BYTES {
        return Err(PosixError::NameTooLong);
    }
    if !(name.starts_with(b"user.")
        || name.starts_with(b"trusted.")
        || name.starts_with(b"security.")
        || name == POSIX_ACL_ACCESS_XATTR
        || name == POSIX_ACL_DEFAULT_XATTR)
    {
        return Err(PosixError::Unsupported);
    }
    Ok(())
}

fn validate_value(kind: FileKind, name: &[u8], value: &[u8]) -> Result<(), PosixError> {
    if value.len() > MAXIMUM_XATTR_VALUE_BYTES {
        return Err(PosixError::TooBig);
    }
    if name == POSIX_ACL_DEFAULT_XATTR && kind != FileKind::Directory {
        return Err(PosixError::PermissionDenied);
    }
    if name == POSIX_ACL_ACCESS_XATTR || name == POSIX_ACL_DEFAULT_XATTR {
        validate_acl(value)?;
    }
    Ok(())
}

fn validate_acl(value: &[u8]) -> Result<(), PosixError> {
    if value.len() < 4 || !(value.len() - 4).is_multiple_of(8) {
        return Err(PosixError::InvalidArgument);
    }
    let version = u32::from_le_bytes(value[0..4].try_into().expect("fixed ACL header"));
    if version != POSIX_ACL_XATTR_VERSION {
        return Err(PosixError::InvalidArgument);
    }
    let mut user_obj = None;
    let mut group_obj = None;
    let mut mask = None;
    let mut other = None;
    let mut named_users = BTreeMap::new();
    let mut named_groups = BTreeMap::new();
    let mut previous_order = None;
    for entry in value[4..].chunks_exact(8) {
        let tag = u16::from_le_bytes(entry[0..2].try_into().expect("fixed ACL tag"));
        let permissions =
            u16::from_le_bytes(entry[2..4].try_into().expect("fixed ACL permission field"));
        let id = u32::from_le_bytes(entry[4..8].try_into().expect("fixed ACL ID"));
        if permissions & !0o7 != 0 {
            return Err(PosixError::InvalidArgument);
        }
        let (order, duplicate) = match tag {
            ACL_USER_OBJ if id == ACL_UNDEFINED_ID => {
                ((0, 0), user_obj.replace(permissions).is_some())
            }
            ACL_USER if id != ACL_UNDEFINED_ID => {
                ((1, id), named_users.insert(id, permissions).is_some())
            }
            ACL_GROUP_OBJ if id == ACL_UNDEFINED_ID => {
                ((2, 0), group_obj.replace(permissions).is_some())
            }
            ACL_GROUP if id != ACL_UNDEFINED_ID => {
                ((3, id), named_groups.insert(id, permissions).is_some())
            }
            ACL_MASK if id == ACL_UNDEFINED_ID => ((4, 0), mask.replace(permissions).is_some()),
            ACL_OTHER if id == ACL_UNDEFINED_ID => ((5, 0), other.replace(permissions).is_some()),
            _ => return Err(PosixError::InvalidArgument),
        };
        if duplicate || previous_order.is_some_and(|previous| previous >= order) {
            return Err(PosixError::InvalidArgument);
        }
        previous_order = Some(order);
    }
    if user_obj.is_none() || group_obj.is_none() || other.is_none() {
        return Err(PosixError::InvalidArgument);
    }
    if (!named_users.is_empty() || !named_groups.is_empty()) && mask.is_none() {
        return Err(PosixError::InvalidArgument);
    }
    Ok(())
}

fn access_acl_mode(value: &[u8]) -> Result<u16, PosixError> {
    validate_acl(value)?;
    let mut owner = None;
    let mut group = None;
    let mut mask = None;
    let mut other = None;
    for entry in value[4..].chunks_exact(8) {
        let tag = u16::from_le_bytes(entry[0..2].try_into().expect("fixed ACL tag"));
        let permissions =
            u16::from_le_bytes(entry[2..4].try_into().expect("fixed ACL permission field"));
        match tag {
            ACL_USER_OBJ => owner = Some(permissions),
            ACL_GROUP_OBJ => group = Some(permissions),
            ACL_MASK => mask = Some(permissions),
            ACL_OTHER => other = Some(permissions),
            _ => {}
        }
    }
    Ok(owner.expect("ASSERT: validated ACL has owner") << 6
        | mask
            .or(group)
            .expect("ASSERT: validated ACL has group class")
            << 3
        | other.expect("ASSERT: validated ACL has other"))
}

fn masked_acl(value: &[u8], mode: u16) -> Result<Vec<u8>, PosixError> {
    validate_acl(value)?;
    let has_mask = value[4..].chunks_exact(8).any(|entry| {
        u16::from_le_bytes(entry[0..2].try_into().expect("fixed ACL tag")) == ACL_MASK
    });
    let mut inherited = value.to_vec();
    for entry in inherited[4..].chunks_exact_mut(8) {
        let tag = u16::from_le_bytes(entry[0..2].try_into().expect("fixed ACL tag"));
        let requested = match tag {
            ACL_USER_OBJ => Some((mode >> 6) & 0o7),
            ACL_MASK => Some((mode >> 3) & 0o7),
            ACL_GROUP_OBJ if !has_mask => Some((mode >> 3) & 0o7),
            ACL_OTHER => Some(mode & 0o7),
            _ => None,
        };
        if let Some(requested) = requested {
            let permissions =
                u16::from_le_bytes(entry[2..4].try_into().expect("fixed ACL permission field"));
            entry[2..4].copy_from_slice(&(permissions & requested).to_le_bytes());
        }
    }
    Ok(inherited)
}

fn rewrite_acl_mode(value: &mut [u8], mode: u16) -> Result<(), PosixError> {
    validate_acl(value)?;
    let has_mask = value[4..].chunks_exact(8).any(|entry| {
        u16::from_le_bytes(entry[0..2].try_into().expect("fixed ACL tag")) == ACL_MASK
    });
    for entry in value[4..].chunks_exact_mut(8) {
        let tag = u16::from_le_bytes(entry[0..2].try_into().expect("fixed ACL tag"));
        let permissions = match tag {
            ACL_USER_OBJ => Some((mode >> 6) & 0o7),
            ACL_MASK => Some((mode >> 3) & 0o7),
            ACL_GROUP_OBJ if !has_mask => Some((mode >> 3) & 0o7),
            ACL_OTHER => Some(mode & 0o7),
            _ => None,
        };
        if let Some(permissions) = permissions {
            entry[2..4].copy_from_slice(&permissions.to_le_bytes());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
        let mut value = POSIX_ACL_XATTR_VERSION.to_le_bytes().to_vec();
        for (tag, permissions, id) in entries {
            value.extend_from_slice(&tag.to_le_bytes());
            value.extend_from_slice(&permissions.to_le_bytes());
            value.extend_from_slice(&id.to_le_bytes());
        }
        value
    }

    #[test]
    fn access_acl_validates_and_projects_the_group_mask_into_mode() {
        let value = acl(&[
            (ACL_USER_OBJ, 0o7, ACL_UNDEFINED_ID),
            (ACL_USER, 0o6, 1_001),
            (ACL_GROUP_OBJ, 0o5, ACL_UNDEFINED_ID),
            (ACL_MASK, 0o4, ACL_UNDEFINED_ID),
            (ACL_OTHER, 0o1, ACL_UNDEFINED_ID),
        ]);
        assert_eq!(access_acl_mode(&value), Ok(0o741));
    }

    #[test]
    fn acl_rejects_duplicates_missing_mask_and_default_on_regular_file() {
        let duplicate_owner = acl(&[
            (ACL_USER_OBJ, 0o7, ACL_UNDEFINED_ID),
            (ACL_USER_OBJ, 0o6, ACL_UNDEFINED_ID),
            (ACL_GROUP_OBJ, 0o5, ACL_UNDEFINED_ID),
            (ACL_OTHER, 0o1, ACL_UNDEFINED_ID),
        ]);
        assert_eq!(
            validate_acl(&duplicate_owner),
            Err(PosixError::InvalidArgument)
        );
        let missing_mask = acl(&[
            (ACL_USER_OBJ, 0o7, ACL_UNDEFINED_ID),
            (ACL_USER, 0o6, 1_001),
            (ACL_GROUP_OBJ, 0o5, ACL_UNDEFINED_ID),
            (ACL_OTHER, 0o1, ACL_UNDEFINED_ID),
        ]);
        assert_eq!(
            validate_acl(&missing_mask),
            Err(PosixError::InvalidArgument)
        );
        assert_eq!(
            ExtendedAttribute::new(
                FileKind::Regular,
                POSIX_ACL_DEFAULT_XATTR.to_vec(),
                acl(&[
                    (ACL_USER_OBJ, 0o7, ACL_UNDEFINED_ID),
                    (ACL_GROUP_OBJ, 0o5, ACL_UNDEFINED_ID),
                    (ACL_OTHER, 0o1, ACL_UNDEFINED_ID),
                ]),
            ),
            Err(PosixError::PermissionDenied)
        );
    }

    #[test]
    fn set_modes_and_accounting_are_bounded_and_deterministic() {
        let mut metadata = InodeMetadata::default();
        assert_eq!(
            metadata.set_xattr(
                FileKind::Regular,
                b"user.immutable.until",
                b"2026-09-01 12:00:00",
                XattrSetMode::Create,
            ),
            Ok(None)
        );
        assert_eq!(
            metadata.set_xattr(
                FileKind::Regular,
                b"user.immutable.until",
                b"later",
                XattrSetMode::Create,
            ),
            Err(PosixError::Exists)
        );
        assert_eq!(
            metadata.list_xattrs(),
            Ok(b"user.immutable.until\0".to_vec())
        );
        metadata
            .remove_xattr(b"user.immutable.until")
            .expect("existing xattr removes");
        assert_eq!(
            metadata.get_xattr(b"user.immutable.until"),
            Err(PosixError::NoData)
        );
    }

    #[test]
    fn default_acl_is_masked_by_creation_mode_and_inherited_by_directories() {
        let default_acl = acl(&[
            (ACL_USER_OBJ, 0o7, ACL_UNDEFINED_ID),
            (ACL_USER, 0o7, 2_000),
            (ACL_GROUP_OBJ, 0o5, ACL_UNDEFINED_ID),
            (ACL_MASK, 0o6, ACL_UNDEFINED_ID),
            (ACL_OTHER, 0o1, ACL_UNDEFINED_ID),
        ]);
        let mut parent = InodeMetadata::default();
        parent
            .set_xattr(
                FileKind::Directory,
                POSIX_ACL_DEFAULT_XATTR,
                &default_acl,
                XattrSetMode::Create,
            )
            .unwrap();
        let (file_mode, file) = parent.for_child(FileKind::Regular, 0o640, 0o077).unwrap();
        assert_eq!(file_mode, 0o640, "default ACL replaces the process umask");
        assert_eq!(
            file.get_xattr(POSIX_ACL_ACCESS_XATTR).unwrap(),
            masked_acl(&default_acl, 0o640).unwrap()
        );
        assert_eq!(
            file.get_xattr(POSIX_ACL_DEFAULT_XATTR),
            Err(PosixError::NoData)
        );
        let (_, directory) = parent.for_child(FileKind::Directory, 0o750, 0o077).unwrap();
        assert_eq!(
            directory.get_xattr(POSIX_ACL_DEFAULT_XATTR).unwrap(),
            default_acl
        );
    }
}

mod short;

pub use self::short::{ShortHash, PREFIX_LENGTH_RANGE};

use std::{fmt, marker::PhantomData, ops::Deref};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    archive::{Archive, ArchiveRecord},
    block::Block,
    entity::Entity,
    error::{Error, Result},
};

pub const SIZE: usize = blake3::OUT_LEN;

#[derive(Debug)]
pub struct Hash<T> {
    inner: blake3::Hash,
    phantom: PhantomData<T>,
}

impl<T> Hash<T> {
    pub const fn from_hash(inner: blake3::Hash) -> Self {
        Hash {
            inner,
            phantom: PhantomData,
        }
    }

    pub const fn from_bytes(bytes: [u8; SIZE]) -> Self {
        let inner = blake3::Hash::from_bytes(bytes);
        Hash::from_hash(inner)
    }

    pub fn format_short(&self, block_count: usize) -> String {
        ShortHash::from_hash(self, block_count).to_string()
    }
}

impl<T> From<blake3::Hash> for Hash<T> {
    fn from(inner: blake3::Hash) -> Self {
        Hash::from_hash(inner)
    }
}

impl Hash<Block> {
    pub fn leaf_block(data: &[u8]) -> Self {
        blake3::hash(data).into()
    }

    pub fn branch_block(children: &[Self]) -> Self {
        let mut hasher = blake3::Hasher::new();

        for hash in children {
            hasher.update(hash.as_bytes());
        }

        hasher.finalize().into()
    }
}

impl Hash<Archive> {
    pub fn archive(archive: &ArchiveRecord) -> Self {
        let mut hasher = blake3::Hasher::new();

        let timestamp = archive.created.format("%+").to_string();
        hasher.update(timestamp.as_bytes());
        hasher.update(&archive.size.to_le_bytes());

        hasher.finalize().into()
    }
}

impl<E: Entity> Hash<E> {
    pub fn key(&self) -> String {
        format!("{}{}", E::KEY_PREFIX, self.inner)
    }

    pub fn from_key(s: &str) -> Result<Self> {
        let hash_str = s
            .strip_prefix(E::KEY_PREFIX)
            .ok_or_else(|| Error::InvalidKey(s.to_owned()))?;
        let inner: blake3::Hash = hash_str
            .parse()
            .map_err(|_| Error::InvalidHash(hash_str.to_owned()))?;
        Ok(inner.into())
    }
}

impl<T> Deref for Hash<T> {
    type Target = blake3::Hash;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Clone for Hash<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Hash<T> {}

impl<T> PartialEq for Hash<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T> Eq for Hash<T> {}

impl<T> std::hash::Hash for Hash<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<T> Serialize for Hash<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Hash<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(blake3::Hash::into)
    }
}

impl<T> fmt::Display for Hash<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

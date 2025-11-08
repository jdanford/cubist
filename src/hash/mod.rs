mod short;

pub use self::short::{PREFIX_LENGTH_RANGE, ShortHash};

use std::{fmt, hash, marker::PhantomData, ops::Deref};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    archive::{Archive, ArchiveRecord},
    block::Block,
    entity::Entity,
    error::{Error, Result},
};

pub const SIZE: usize = blake3::OUT_LEN;

#[derive(Debug)]
pub struct Hash<E> {
    inner: blake3::Hash,
    phantom: PhantomData<E>,
}

impl<E> Hash<E> {
    const fn from_inner(inner: blake3::Hash) -> Self {
        Hash {
            inner,
            phantom: PhantomData,
        }
    }

    pub const fn from_bytes(bytes: [u8; SIZE]) -> Self {
        let inner = blake3::Hash::from_bytes(bytes);
        Hash::from_inner(inner)
    }

    pub fn format_short(&self, block_count: usize) -> String {
        ShortHash::from_hash(self, block_count).into()
    }
}

impl<E> From<blake3::Hash> for Hash<E> {
    fn from(inner: blake3::Hash) -> Self {
        Hash::from_inner(inner)
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
    pub fn from_key(s: &str) -> Result<Self> {
        let hash_str = s
            .strip_prefix(E::KEY_PREFIX)
            .ok_or_else(|| Error::InvalidKey(s.to_owned()))?;
        let inner = hash_str
            .parse::<blake3::Hash>()
            .map_err(|_| Error::InvalidHash(hash_str.to_owned()))?;
        Ok(inner.into())
    }

    pub fn key(&self) -> String {
        format!("{}{}", E::KEY_PREFIX, self.inner)
    }
}

impl<E> Deref for Hash<E> {
    type Target = blake3::Hash;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<E> Clone for Hash<E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Copy for Hash<E> {}

impl<E> PartialEq for Hash<E> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<E> Eq for Hash<E> {}

impl<E> hash::Hash for Hash<E> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<E> Serialize for Hash<E> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.inner.serialize(serializer)
    }
}

impl<'de, E> Deserialize<'de> for Hash<E> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Deserialize::deserialize(deserializer).map(Hash::from_inner)
    }
}

impl<E> fmt::Display for Hash<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

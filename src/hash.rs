use std::{fmt, marker::PhantomData, ops::Deref};

use crate::block::HASH_SIZE;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hash<T>(blake3::Hash, PhantomData<T>);

impl<T> Deref for Hash<T> {
    type Target = blake3::Hash;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<blake3::Hash> for Hash<T> {
    fn from(hash: blake3::Hash) -> Self {
        Hash(hash, PhantomData)
    }
}

impl<T> Hash<T> {
    pub fn from_bytes(bytes: [u8; HASH_SIZE]) -> Self {
        Hash(blake3::Hash::from_bytes(bytes), PhantomData)
    }
}

impl<T> fmt::Display for Hash<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl<T> fmt::Debug for Hash<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = self.0.to_hex();
        let hex: &str = hex.as_str();

        f.debug_tuple("Hash").field(&hex).finish()
    }
}

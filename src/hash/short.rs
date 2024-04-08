use std::{fmt, marker::PhantomData, ops::RangeInclusive, str::FromStr};

use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};

use crate::error::{Error, Result};

use super::{Entity, Hash, SIZE};

const MIN_PREFIX_LENGTH: usize = 6;
const MAX_PREFIX_LENGTH: usize = SIZE * 2;
pub const PREFIX_LENGTH_RANGE: RangeInclusive<usize> = MIN_PREFIX_LENGTH..=MAX_PREFIX_LENGTH;

static HEX_REGEX: Lazy<Regex> = Lazy::new(|| {
    RegexBuilder::new(r"^[0-9a-f]+$")
        .case_insensitive(true)
        .build()
        .unwrap()
});

#[derive(Debug)]
pub struct ShortHash<E> {
    inner: String,
    phantom: PhantomData<E>,
}

impl<E> ShortHash<E> {
    fn from_string(s: String) -> Self {
        ShortHash {
            inner: s,
            phantom: PhantomData,
        }
    }

    pub fn new(s: String) -> Result<Self> {
        if PREFIX_LENGTH_RANGE.contains(&s.len()) && HEX_REGEX.is_match(&s) {
            Ok(ShortHash::from_string(s))
        } else {
            Err(Error::InvalidHash(s))
        }
    }

    pub fn from_hash(hash: &Hash<E>, block_count: usize) -> Self {
        let len = safe_prefix_length(block_count);
        let s = hash.to_hex()[..len].to_string();
        ShortHash::from_string(s)
    }
}

impl<E> From<ShortHash<E>> for String {
    fn from(hash: ShortHash<E>) -> String {
        hash.inner
    }
}

impl<E: Entity> ShortHash<E> {
    pub fn key_prefix(&self) -> String {
        format!("{}{}", E::KEY_PREFIX, self.inner)
    }
}

impl<E> Clone for ShortHash<E> {
    fn clone(&self) -> Self {
        ShortHash {
            inner: self.inner.clone(),
            phantom: PhantomData,
        }
    }
}

impl<E> PartialEq for ShortHash<E> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<E> Eq for ShortHash<E> {}

impl<E> FromStr for ShortHash<E> {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ShortHash::new(s.to_owned())
    }
}

impl<E> fmt::Display for ShortHash<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
fn safe_prefix_length(block_count: usize) -> usize {
    // see https://github.com/git/git/commit/e6c587c733b4634030b353f4024794b08bc86892
    // 2^(2N) > block_count
    //     2N > log2(block_count)
    //      N > log2(block_count) / 2
    //      N = log2(block_count) / 2 + 1
    let bits = (block_count as f64).log2() / 2.0 + 1.0;
    let chars = bits / 4.0;
    let len = chars.ceil() as usize;
    len.clamp(MIN_PREFIX_LENGTH, MAX_PREFIX_LENGTH)
}

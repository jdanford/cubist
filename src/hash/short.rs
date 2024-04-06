use std::{fmt, marker::PhantomData, ops::RangeInclusive, str::FromStr};

use once_cell::sync::Lazy;
use regex::Regex;

use crate::error::{Error, Result};

use super::{Entity, Hash, SIZE};

const MIN_PREFIX_LENGTH: usize = 6;
const MAX_PREFIX_LENGTH: usize = SIZE * 2;
pub const PREFIX_LENGTH_RANGE: RangeInclusive<usize> = MIN_PREFIX_LENGTH..=MAX_PREFIX_LENGTH;

static HEX_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[0-9a-fA-F]+$").unwrap());

#[derive(Debug)]
pub struct ShortHash<T> {
    inner: String,
    phantom: PhantomData<T>,
}

impl<T> ShortHash<T> {
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

    pub fn from_hash(hash: &Hash<T>, block_count: usize) -> Self {
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

impl<T> Clone for ShortHash<T> {
    fn clone(&self) -> Self {
        ShortHash {
            inner: self.inner.clone(),
            phantom: PhantomData,
        }
    }
}

impl<T> PartialEq for ShortHash<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T> Eq for ShortHash<T> {}

impl<T> FromStr for ShortHash<T> {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ShortHash::new(s.to_owned())
    }
}

impl<T> fmt::Display for ShortHash<T> {
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

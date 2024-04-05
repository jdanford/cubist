use std::{fmt, ops::RangeInclusive, str::FromStr};

use once_cell::sync::Lazy;
use regex::Regex;

use crate::{
    archive::ArchiveRecord,
    error::{Error, Result},
};

pub const SIZE: usize = blake3::OUT_LEN;

static HASH_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[0-9a-fA-F]+$").unwrap());

pub type Hash = blake3::Hash;

#[derive(Debug, Clone)]
pub struct ShortHash(String);

impl ShortHash {
    pub fn new(s: String) -> Result<Self> {
        if PREFIX_LENGTH_RANGE.contains(&s.len()) && HASH_REGEX.is_match(&s) {
            Ok(ShortHash(s))
        } else {
            Err(Error::InvalidHash(s))
        }
    }
}

impl FromStr for ShortHash {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ShortHash::new(s.to_owned())
    }
}

impl fmt::Display for ShortHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub fn leaf(data: &[u8]) -> Hash {
    blake3::hash(data)
}

pub fn branch(children: &[Hash]) -> Hash {
    let mut hasher = blake3::Hasher::new();

    for hash in children {
        hasher.update(hash.as_bytes());
    }

    hasher.finalize()
}

pub fn archive(archive: &ArchiveRecord) -> Hash {
    let mut hasher = blake3::Hasher::new();

    let timestamp = archive.created.format("%+").to_string();
    hasher.update(timestamp.as_bytes());
    hasher.update(&archive.size.to_le_bytes());

    hasher.finalize()
}

pub fn concat<I>(hashes: I) -> Vec<u8>
where
    I: IntoIterator<Item = Hash>,
{
    hashes
        .into_iter()
        .flat_map(|hash| *hash.as_bytes())
        .collect()
}

pub fn split(bytes: &[u8]) -> impl Iterator<Item = Hash> + '_ {
    bytes
        .chunks_exact(SIZE)
        .map(|bytes| Hash::from_bytes(bytes.try_into().unwrap()))
}

pub fn format_short(hash: &Hash, block_count: usize) -> String {
    let len = safe_prefix_length(block_count);
    hash.to_hex()[..len].to_string()
}

pub const MIN_PREFIX_LENGTH: usize = 6;
pub const MAX_PREFIX_LENGTH: usize = SIZE * 2;
pub const PREFIX_LENGTH_RANGE: RangeInclusive<usize> = MIN_PREFIX_LENGTH..=MAX_PREFIX_LENGTH;

#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
pub fn safe_prefix_length(block_count: usize) -> usize {
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

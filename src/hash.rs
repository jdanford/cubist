use std::{fmt, ops::RangeInclusive, str::FromStr};

use rand::RngCore;

use crate::error::Error;

pub const SIZE: usize = blake3::OUT_LEN;

pub type Hash = blake3::Hash;

#[derive(Debug, Clone)]
pub struct ShortHash(Vec<u8>);

impl ShortHash {
    #[allow(dead_code)]
    pub fn matches(&self, hash: &Hash) -> bool {
        let short_bytes = self.as_bytes();
        let long_bytes = hash.as_bytes();
        short_bytes == &long_bytes[..short_bytes.len()]
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0[..]
    }
}

impl FromStr for ShortHash {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !PREFIX_LENGTH_RANGE.contains(&s.len()) {
            return Err(Error::InvalidHash(s.to_owned()));
        }

        let bytes = hex::decode(s).map_err(|_| Error::InvalidHash(s.to_owned()))?;
        Ok(ShortHash(bytes))
    }
}

impl fmt::Display for ShortHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.as_bytes()))
    }
}

pub fn random() -> Hash {
    let mut bytes = [0; SIZE];
    rand::thread_rng().fill_bytes(&mut bytes);
    Hash::from_bytes(bytes)
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

const MIN_PREFIX_LENGTH: usize = 6;
const MAX_PREFIX_LENGTH: usize = SIZE * 2;
pub const PREFIX_LENGTH_RANGE: RangeInclusive<usize> = MIN_PREFIX_LENGTH..=MAX_PREFIX_LENGTH;

#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
pub fn safe_prefix_length(block_count: usize) -> usize {
    // see `https://github.com/git/git/commit/e6c587c733b4634030b353f4024794b08bc86892`
    // 2^(2N) > block_count
    //     2N > log2(block_count)
    //      N > log2(block_count) / 2
    //      N = log2(block_count) / 2 + 1
    let bits = (block_count as f64).log2() / 2.0 + 1.0;
    let chars = bits / 4.0;
    let len = chars.ceil() as usize;
    len.clamp(MIN_PREFIX_LENGTH, MAX_PREFIX_LENGTH)
}

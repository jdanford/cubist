pub const SIZE: usize = blake3::OUT_LEN;

pub type Hash = blake3::Hash;

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

#[allow(dead_code)]
pub fn format_short(hash: &Hash, block_count: usize) -> String {
    let len = safe_prefix_length(block_count);
    hash.to_hex()[..len].to_string()
}

const MIN_PREFIX_LENGTH: usize = 6;

#[allow(
    dead_code,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
pub fn safe_prefix_length(block_count: usize) -> usize {
    // 2^(2N) = block_count
    // 2N = log2(block_count)
    // N = log2(block_count) / 2
    let bits_partial = (block_count as f64).log2() / 2.0;
    let chars_partial = bits_partial / 4.0;
    let len = chars_partial.ceil() as usize;
    len.max(MIN_PREFIX_LENGTH)
}

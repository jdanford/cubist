pub const SIZE: usize = blake3::OUT_LEN;

const PLACEHOLDER: &str = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

pub type Hash = blake3::Hash;
pub type Hasher = blake3::Hasher;

pub fn concat<I>(hashes: I) -> Vec<u8>
where
    I: Iterator<Item = Hash>,
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

pub fn format(maybe_hash: &Option<Hash>) -> String {
    maybe_hash.map_or_else(|| PLACEHOLDER.to_string(), |hash| hash.to_string())
}

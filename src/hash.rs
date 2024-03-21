use std::borrow::Cow;

pub const SIZE: usize = blake3::OUT_LEN;

const PLACEHOLDER: &str = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

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

pub fn format(maybe_hash: &Option<Hash>) -> Cow<str> {
    maybe_hash.map_or_else(
        || Cow::Borrowed(PLACEHOLDER),
        |hash| Cow::Owned(hash.to_string()),
    )
}

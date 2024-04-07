use crate::error::Result;

pub fn compress(bytes: &[u8], level: u8) -> Result<Vec<u8>> {
    let compressed_bytes = zstd::encode_all(bytes, level.into())?;
    Ok(compressed_bytes)
}

pub fn decompress(compressed_bytes: &[u8]) -> Result<Vec<u8>> {
    let bytes = zstd::decode_all(compressed_bytes)?;
    Ok(bytes)
}

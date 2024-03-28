use crate::error::Result;

pub fn compress(data: &[u8], level: u8) -> Result<Vec<u8>> {
    let compressed_data = zstd::encode_all(data, level.into())?;
    Ok(compressed_data)
}

pub fn decompress(compressed_data: &[u8]) -> Result<Vec<u8>> {
    let data = zstd::decode_all(compressed_data)?;
    Ok(data)
}

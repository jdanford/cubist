use std::io::{Cursor, Read, Write};

use fastcdc::v2020::AsyncStreamCDC;
use tokio::{io::AsyncRead, task::yield_now};

use crate::{
    error::{Error, Result},
    hash::Hash,
};

pub const COPY_CHUNK_SIZE: usize = 8 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Block;

pub type BlockHash = Hash<Block>;

impl BlockHash {
    pub fn key(&self) -> String {
        format!("block:{self}")
    }
}

pub async fn hash(data: &[u8]) -> Result<BlockHash> {
    let mut hasher = blake3::Hasher::new();

    for chunk in data.chunks(COPY_CHUNK_SIZE) {
        hasher.write_all(chunk)?;
        yield_now().await;
    }

    let hash = hasher.finalize().into();
    Ok(hash)
}

pub async fn compress(data: &[u8], compression_level: u32) -> Result<Vec<u8>> {
    let buffer = Vec::with_capacity(data.len() / 2);
    let mut encoder = lz4::EncoderBuilder::new()
        .level(compression_level)
        .build(buffer)?;

    for chunk in data.chunks(COPY_CHUNK_SIZE) {
        encoder.write_all(chunk)?;
        yield_now().await;
    }

    let (compressed_data, result) = encoder.finish();
    result?;

    Ok(compressed_data)
}

pub async fn decompress_and_verify(
    expected_hash: &BlockHash,
    compressed_data: &[u8],
) -> Result<Vec<u8>> {
    let reader = Cursor::new(compressed_data);
    let mut decoder = lz4::Decoder::new(reader)?;

    let mut hasher = blake3::Hasher::new();
    let mut data = Vec::new();

    let mut buffer = vec![0; COPY_CHUNK_SIZE];
    loop {
        let n = decoder.read(&mut buffer)?;
        if n == 0 {
            break;
        }

        hasher.write_all(&buffer[..n])?;
        yield_now().await;

        data.write_all(&buffer[..n])?;
        yield_now().await;
    }

    let actual_hash: BlockHash = hasher.finalize().into();
    if &actual_hash != expected_hash {
        return Err(Error::WrongBlockHash {
            actual: actual_hash,
            expected: *expected_hash,
        });
    }

    Ok(data)
}

pub fn chunker<R: AsyncRead + Unpin>(reader: R, target_block_size: u32) -> AsyncStreamCDC<R> {
    let min_block_size = target_block_size / 2;
    let max_block_size = target_block_size * 4;
    AsyncStreamCDC::new(reader, min_block_size, target_block_size, max_block_size)
}

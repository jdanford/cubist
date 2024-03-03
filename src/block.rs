use std::io::Cursor;

use fastcdc::v2020::AsyncStreamCDC;
use tokio::{
    io::{AsyncRead, AsyncWriteExt},
    task::spawn_blocking,
};

use crate::{
    error::{Error, Result},
    hash::{self, Hash, Hasher},
    storage,
};

pub enum Block {
    Leaf {
        hash: Hash,
        data: Vec<u8>,
    },
    Branch {
        hash: Hash,
        level: u8,
        children: Vec<Hash>,
    },
}

impl Block {
    pub async fn leaf(data: Vec<u8>) -> Result<Self> {
        let (data, hash) = spawn_blocking(move || {
            let hash = blake3::hash(&data);
            (data, hash)
        })
        .await?;

        Ok(Block::Leaf { hash, data })
    }

    pub async fn branch(level: u8, children: Vec<Hash>) -> Result<Self> {
        let (children, hash) = spawn_blocking(move || {
            let hash = hash_children(&children);
            (children, hash)
        })
        .await?;

        Ok(Block::Branch {
            hash,
            level,
            children,
        })
    }

    pub fn hash(&self) -> &Hash {
        match self {
            Block::Leaf { hash, .. } | Block::Branch { hash, .. } => hash,
        }
    }

    pub fn storage_key(&self) -> String {
        storage::block_key(self.hash())
    }

    pub async fn encode(self, compression_level: u32) -> Result<Vec<u8>> {
        let raw_block = self.into_raw(compression_level).await?;
        raw_block.encode().await
    }

    pub async fn decode(
        expected_hash: Hash,
        expected_level: Option<u8>,
        bytes: &[u8],
    ) -> Result<Self> {
        let raw_block = RawBlock::decode(expected_level, bytes)?;
        Block::from_raw(raw_block, &expected_hash).await
    }

    async fn into_raw(self, compression_level: u32) -> Result<RawBlock> {
        match self {
            Block::Leaf { data, .. } => {
                let bytes = spawn_blocking(move || compress(&data, compression_level)).await??;
                Ok(RawBlock { level: 0, bytes })
            }
            Block::Branch {
                level, children, ..
            } => {
                let bytes = hash::concat(children);
                Ok(RawBlock { level, bytes })
            }
        }
    }

    async fn from_raw(raw_block: RawBlock, expected_hash: &Hash) -> Result<Self> {
        let RawBlock { level, bytes } = raw_block;
        if level == 0 {
            let data = spawn_blocking(move || decompress(&bytes)).await??;
            let (data, hash) = spawn_blocking(move || {
                let hash = blake3::hash(&data);
                (data, hash)
            })
            .await?;

            assert_hash(&hash, expected_hash)?;
            Ok(Block::Leaf { hash, data })
        } else {
            if bytes.len() % hash::SIZE != 0 {
                return Err(Error::BlockNotLongEnough);
            }

            let children = hash::split(&bytes).collect::<Vec<_>>();
            let (children, hash) = spawn_blocking(move || {
                let hash = hash_children(&children);
                (children, hash)
            })
            .await?;

            assert_hash(&hash, expected_hash)?;
            Ok(Block::Branch {
                hash,
                level,
                children,
            })
        }
    }

    pub fn storage_key_for_hash(hash: &Hash) -> String {
        format!("block:{hash}")
    }
}

struct RawBlock {
    level: u8,
    bytes: Vec<u8>,
}

impl RawBlock {
    pub async fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = vec![];
        bytes.write_u8(self.level).await?;
        bytes.write_all(&self.bytes).await?;
        Ok(bytes)
    }

    pub fn decode(expected_level: Option<u8>, bytes: &[u8]) -> Result<Self> {
        let (&level, bytes) = bytes.split_first().ok_or(Error::BlockNotLongEnough)?;
        assert_level(level, expected_level)?;
        Ok(RawBlock {
            level,
            bytes: bytes.to_owned(),
        })
    }
}

fn compress(data: &[u8], compression_level: u32) -> Result<Vec<u8>> {
    let mut reader = Cursor::new(data);
    let buffer = Vec::with_capacity(data.len() / 2);
    let mut encoder = lz4::EncoderBuilder::new()
        .checksum(lz4::ContentChecksum::NoChecksum)
        .level(compression_level)
        .build(buffer)?;

    std::io::copy(&mut reader, &mut encoder)?;
    let (compressed_data, result) = encoder.finish();
    result?;

    Ok(compressed_data)
}

fn decompress(compressed_data: &[u8]) -> Result<Vec<u8>> {
    let reader = Cursor::new(compressed_data);
    let mut decoder = lz4::Decoder::new(reader)?;
    let mut data = vec![];

    std::io::copy(&mut decoder, &mut data)?;
    Ok(data)
}

pub fn chunker<R: AsyncRead + Unpin>(reader: R, target_size: u32) -> AsyncStreamCDC<R> {
    let min_size = target_size / 2;
    let max_size = target_size * 4;
    AsyncStreamCDC::new(reader, min_size, target_size, max_size)
}

pub fn assert_level(actual: u8, expected: Option<u8>) -> Result<()> {
    if let Some(expected) = expected {
        if expected != actual {
            return Err(Error::WrongBlockLevel { actual, expected });
        }
    }

    Ok(())
}

fn assert_hash(actual: &Hash, expected: &Hash) -> Result<()> {
    if expected != actual {
        return Err(Error::WrongBlockHash {
            actual: *actual,
            expected: *expected,
        });
    }

    Ok(())
}

fn hash_children(children: &[Hash]) -> Hash {
    let mut hasher = Hasher::new();
    for hash in children {
        hasher.update(hash.as_bytes());
    }

    hasher.finalize()
}

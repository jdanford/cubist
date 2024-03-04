use fastcdc::v2020::AsyncStreamCDC;
use tokio::{
    io::{AsyncRead, AsyncWriteExt},
    task::spawn_blocking,
};

use crate::{
    error::{Error, Result},
    hash::{self, Hash},
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
            let hash = hash::branch(&children);
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

    pub async fn encode(self, compression_level: u8) -> Result<Vec<u8>> {
        let (level, bytes) = self.into_raw(compression_level).await?;
        let mut buf = vec![];
        buf.write_u8(level).await?;
        buf.write_all(&bytes).await?;
        Ok(buf)
    }

    pub async fn decode(
        expected_hash: Hash,
        expected_level: Option<u8>,
        bytes: &[u8],
    ) -> Result<Self> {
        let (&level, bytes) = bytes
            .split_first()
            .ok_or_else(|| Error::InvalidBlockSize(0))?;
        assert_level(level, expected_level)?;
        Block::from_raw(&expected_hash, level, bytes.to_owned()).await
    }

    async fn into_raw(self, compression_level: u8) -> Result<(u8, Vec<u8>)> {
        match self {
            Block::Leaf { data, .. } => {
                let bytes = spawn_blocking(move || compress(&data, compression_level)).await??;
                Ok((0, bytes))
            }
            Block::Branch {
                level, children, ..
            } => {
                let bytes = hash::concat(children);
                Ok((level, bytes))
            }
        }
    }

    async fn from_raw(expected_hash: &Hash, level: u8, bytes: Vec<u8>) -> Result<Self> {
        let block = if level == 0 {
            Block::leaf_from_raw(bytes).await?
        } else {
            Block::branch_from_raw(level, bytes).await?
        };

        assert_hash(block.hash(), expected_hash)?;
        Ok(block)
    }

    async fn leaf_from_raw(bytes: Vec<u8>) -> Result<Self> {
        let data = spawn_blocking(move || decompress(&bytes)).await??;
        let (data, hash) = spawn_blocking(move || {
            let hash = hash::leaf(&data);
            (data, hash)
        })
        .await?;

        Ok(Block::Leaf { hash, data })
    }

    async fn branch_from_raw(level: u8, bytes: Vec<u8>) -> Result<Self> {
        let size = bytes.len();
        if size % hash::SIZE != 0 {
            return Err(Error::InvalidBlockSize(size));
        }

        let children = hash::split(&bytes).collect::<Vec<_>>();
        let (children, hash) = spawn_blocking(move || {
            let hash = hash::branch(&children);
            (children, hash)
        })
        .await?;

        Ok(Block::Branch {
            hash,
            level,
            children,
        })
    }
}

fn compress(data: &[u8], level: u8) -> Result<Vec<u8>> {
    let compressed_data = zstd::encode_all(data, level.into())?;
    Ok(compressed_data)
}

fn decompress(compressed_data: &[u8]) -> Result<Vec<u8>> {
    let data = zstd::decode_all(compressed_data)?;
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

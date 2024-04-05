mod records;
#[cfg(test)]
mod tests;

use std::borrow::Borrow;

use fastcdc::v2020::AsyncStreamCDC;
use tokio::{
    io::{AsyncRead, AsyncWriteExt},
    task::spawn_blocking,
};

use crate::{
    compression::{compress, decompress},
    entity::Entity,
    error::{assert_block_level_eq, assert_hash_eq, assert_size_multiple_of_hash, Error, Result},
    hash::{self, Hash},
};

pub use self::records::{BlockRecord, BlockRecords, BlockRefs};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Block {
    Leaf {
        hash: Hash<Block>,
        data: Vec<u8>,
    },
    Branch {
        hash: Hash<Block>,
        level: u8,
        children: Vec<Hash<Block>>,
    },
}

impl Entity for Block {
    const NAME: &'static str = "block";
    const KEY_PREFIX: &'static str = "blocks/";
}

impl Block {
    pub async fn leaf(data: Vec<u8>) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::EmptyBlock);
        }

        let (data, hash) = spawn_blocking(move || {
            let hash = Hash::leaf_block(&data);
            (data, hash)
        })
        .await?;

        Ok(Block::Leaf { hash, data })
    }

    pub async fn branch(level: u8, children: Vec<Hash<Block>>) -> Result<Self> {
        if level == 0 {
            return Err(Error::BranchLevelZero);
        }

        if children.is_empty() {
            return Err(Error::EmptyBlock);
        }

        let (children, hash) = spawn_blocking(move || {
            let hash = Hash::branch_block(&children);
            (children, hash)
        })
        .await?;

        Ok(Block::Branch {
            hash,
            level,
            children,
        })
    }

    #[allow(dead_code)]
    pub fn level(&self) -> u8 {
        match self {
            Block::Leaf { .. } => 0,
            Block::Branch { level, .. } => *level,
        }
    }

    pub fn hash(&self) -> &Hash<Block> {
        match self {
            Block::Leaf { hash, .. } | Block::Branch { hash, .. } => hash,
        }
    }

    pub async fn encode(self, compression_level: u8) -> Result<Vec<u8>> {
        let (level, bytes) = self.into_raw(compression_level).await?;
        let mut buf = vec![];
        buf.write_u8(level).await?;
        buf.write_all(&bytes).await?;
        Ok(buf)
    }

    pub async fn decode(
        expected_hash: &Hash<Block>,
        expected_level: Option<u8>,
        bytes: &[u8],
    ) -> Result<Self> {
        let (&level, bytes) = bytes
            .split_first()
            .ok_or_else(|| Error::InvalidBlockSize(0))?;
        assert_block_level_eq(*expected_hash, level, expected_level)?;
        Block::from_raw(expected_hash, level, bytes.to_owned()).await
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
                let bytes = concat(children);
                Ok((level, bytes))
            }
        }
    }

    async fn from_raw(expected_hash: &Hash<Block>, level: u8, bytes: Vec<u8>) -> Result<Self> {
        let block = if level == 0 {
            Block::leaf_from_raw(bytes).await?
        } else {
            Block::branch_from_raw(level, bytes).await?
        };

        assert_hash_eq(block.hash(), expected_hash)?;
        Ok(block)
    }

    async fn leaf_from_raw(bytes: Vec<u8>) -> Result<Self> {
        let data = spawn_blocking(move || decompress(&bytes)).await??;
        let (data, hash) = spawn_blocking(move || {
            let hash = Hash::leaf_block(&data);
            (data, hash)
        })
        .await?;

        Ok(Block::Leaf { hash, data })
    }

    async fn branch_from_raw(level: u8, bytes: Vec<u8>) -> Result<Self> {
        let size = bytes.len() as u64;
        assert_size_multiple_of_hash(size)?;

        let children = split(&bytes).collect::<Vec<_>>();
        let (children, hash) = spawn_blocking(move || {
            let hash = Hash::branch_block(&children);
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

pub fn chunker<R: AsyncRead + Unpin>(reader: R, target_size: u32) -> AsyncStreamCDC<R> {
    let min_size = target_size / 2;
    let max_size = target_size * 4;
    AsyncStreamCDC::new(reader, min_size, target_size, max_size)
}

pub fn concat<H, I>(hashes: I) -> Vec<u8>
where
    H: Borrow<Hash<Block>>,
    I: IntoIterator<Item = H>,
{
    let mut bytes = vec![];
    for hash in hashes {
        bytes.extend(hash.borrow().as_bytes());
    }

    bytes
}

pub fn split(bytes: &[u8]) -> impl Iterator<Item = Hash<Block>> + '_ {
    bytes
        .chunks_exact(hash::SIZE)
        .map(|bytes| Hash::from_bytes(bytes.try_into().unwrap()))
}

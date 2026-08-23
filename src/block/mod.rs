mod records;
#[cfg(test)]
mod tests;

use std::borrow::Borrow;

use crate::{
    assert::{assert_block_level_eq, assert_hash_eq, assert_size_multiple_of_hash},
    compress::{compress, decompress},
    entity::Entity,
    error::{Error, Result},
    hash::Hash,
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
    pub fn leaf(data: Vec<u8>) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::EmptyBlock);
        }

        let hash = Hash::leaf_block(&data);
        Ok(Block::Leaf { hash, data })
    }

    pub fn branch(level: u8, children: Vec<Hash<Block>>) -> Result<Self> {
        if level == 0 {
            return Err(Error::BranchLevelZero);
        }

        if children.is_empty() {
            return Err(Error::EmptyBlock);
        }

        let hash = Hash::branch_block(&children);
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

    pub fn encode(self, compression_level: u8) -> Result<Vec<u8>> {
        let (level, bytes) = self.into_raw(compression_level)?;
        let mut buf = vec![];
        buf.push(level);
        buf.extend(&bytes);
        Ok(buf)
    }

    pub fn decode(
        expected_hash: &Hash<Block>,
        expected_level: Option<u8>,
        bytes: &[u8],
    ) -> Result<Self> {
        let (&level, bytes) = bytes
            .split_first()
            .ok_or_else(|| Error::InvalidBlockSize(0))?;
        assert_block_level_eq(expected_hash, level, expected_level)?;
        Block::from_raw(expected_hash, level, bytes)
    }

    fn from_raw(expected_hash: &Hash<Block>, level: u8, bytes: &[u8]) -> Result<Self> {
        let block = if level == 0 {
            let data = decompress(bytes)?;
            Self::leaf(data)?
        } else {
            let children = split(bytes)?.collect::<Vec<_>>();
            Self::branch(level, children)?
        };

        assert_hash_eq(block.hash(), expected_hash)?;
        Ok(block)
    }

    fn into_raw(self, compression_level: u8) -> Result<(u8, Vec<u8>)> {
        match self {
            Block::Leaf { data, .. } => {
                let bytes = compress(&data, compression_level)?;
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
}

fn split(bytes: &[u8]) -> Result<impl Iterator<Item = Hash<Block>> + '_> {
    let size = bytes.len() as u64;
    assert_size_multiple_of_hash(size)?;

    let (chunks, _) = bytes.as_chunks();
    Ok(chunks.iter().map(|&bytes| Hash::from_bytes(bytes)))
}

fn concat<H, I>(hashes: I) -> Vec<u8>
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

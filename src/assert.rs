use crate::{
    block::Block,
    error::{Error, Result},
    hash::{self, Hash},
};

pub fn assert_block_level_eq(hash: Hash<Block>, actual: u8, expected: Option<u8>) -> Result<()> {
    if let Some(expected) = expected {
        if expected != actual {
            return Err(Error::WrongBlockLevel {
                hash,
                actual,
                expected,
            });
        }
    }

    Ok(())
}

pub fn assert_hash_eq(actual: &Hash<Block>, expected: &Hash<Block>) -> Result<()> {
    if expected != actual {
        return Err(Error::WrongBlockHash {
            actual: *actual,
            expected: *expected,
        });
    }

    Ok(())
}

pub fn assert_size_multiple_of_hash(size: u64) -> Result<()> {
    if size % hash::SIZE as u64 != 0 {
        return Err(Error::InvalidBlockSize(size));
    }

    Ok(())
}

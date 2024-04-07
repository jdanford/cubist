use crate::{
    block::Block,
    error::Error,
    hash::{self, Hash},
};

pub const COMPRESSION_LEVEL: u8 = 3;
pub const NULL_HASH: Hash<Block> = Hash::from_bytes([0; hash::SIZE]);

fn roundtrip_block(block: &Block) -> Block {
    let bytes = block.clone().encode(COMPRESSION_LEVEL).unwrap();
    Block::decode(block.hash(), Some(block.level()), &bytes).unwrap()
}

#[test]
fn block_leaf_roundtrip() {
    let block = Block::leaf(vec![0; 32]).unwrap();
    assert_eq!(block, roundtrip_block(&block));
}

#[test]
fn block_leaf_empty_error() {
    assert_eq!(Block::leaf(vec![]), Err(Error::EmptyBlock));
}

#[test]
fn block_branch_0_error() {
    assert_eq!(
        Block::branch(0, vec![NULL_HASH]),
        Err(Error::BranchLevelZero)
    );
}

#[test]
fn block_branch_1_roundtrip() {
    let block = Block::branch(1, vec![NULL_HASH]).unwrap();
    assert_eq!(block, roundtrip_block(&block));
}

#[test]
fn block_branch_1_empty_error() {
    assert_eq!(Block::branch(1, vec![]), Err(Error::EmptyBlock));
}

#[test]
fn block_branch_2_roundtrip() {
    let block = Block::branch(2, vec![NULL_HASH]).unwrap();
    assert_eq!(block, roundtrip_block(&block));
}

#[test]
fn block_branch_255_roundtrip() {
    let block = Block::branch(255, vec![NULL_HASH]).unwrap();
    assert_eq!(block, roundtrip_block(&block));
}

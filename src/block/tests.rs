use crate::{
    block::Block,
    hash::{self, Hash},
};

pub const COMPRESSION_LEVEL: u8 = 3;
pub const NULL_HASH: Hash = Hash::from_bytes([0; hash::SIZE]);

async fn roundtrip_block(block: &Block) -> Block {
    let bytes = block.clone().encode(COMPRESSION_LEVEL).await.unwrap();
    Block::decode(block.hash(), Some(block.level()), &bytes)
        .await
        .unwrap()
}

#[tokio::test]
async fn block_leaf_roundtrip() {
    let block = Block::leaf(vec![0; 1024]).await.unwrap();
    assert_eq!(block, roundtrip_block(&block).await);
}

#[tokio::test]
async fn block_leaf_empty_error() {
    // Error::EmptyBlock
    assert!(Block::leaf(vec![]).await.is_err());
}

#[tokio::test]
async fn block_branch_0_error() {
    // Error::BranchLevelZero
    assert!(Block::branch(0, vec![NULL_HASH]).await.is_err());
}

#[tokio::test]
async fn block_branch_1_roundtrip() {
    let block = Block::branch(1, vec![NULL_HASH]).await.unwrap();
    assert_eq!(block, roundtrip_block(&block).await);
}

#[tokio::test]
async fn block_branch_1_empty_error() {
    // Error::EmptyBlock
    assert!(Block::branch(1, vec![]).await.is_err());
}

#[tokio::test]
async fn block_branch_2_roundtrip() {
    let block = Block::branch(2, vec![NULL_HASH]).await.unwrap();
    assert_eq!(block, roundtrip_block(&block).await);
}

#[tokio::test]
async fn block_branch_255_roundtrip() {
    let block = Block::branch(255, vec![NULL_HASH]).await.unwrap();
    assert_eq!(block, roundtrip_block(&block).await);
}

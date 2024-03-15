use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    block::Block,
    error::{Error, Result},
    hash::{self, Hash},
};

use super::{Args, State};

pub struct UploadTree {
    args: Arc<Args>,
    state: Arc<RwLock<State>>,
    layers: Vec<Vec<Hash>>,
}

impl UploadTree {
    pub fn new(args: Arc<Args>, state: Arc<RwLock<State>>) -> Self {
        UploadTree {
            args,
            state,
            layers: vec![],
        }
    }

    pub async fn add_leaf(&mut self, data: Vec<u8>) -> Result<()> {
        let block = Block::leaf(data).await?;
        let hash = upload_block(self.args.clone(), self.state.clone(), block).await?;
        self.state.write().await.archive.add_ref(&hash);
        self.add_inner(hash, false).await?;
        Ok(())
    }

    pub async fn finalize(mut self) -> Result<Option<Hash>> {
        if self.layers.is_empty() {
            return Ok(None);
        }

        let bottom_layer = self.layers.first_mut().unwrap();
        let hash = bottom_layer
            .pop()
            .expect("bottom layer should not be empty");
        self.add_inner(hash, true).await?;

        let top_layer = self.layers.last().unwrap();
        let hash = *top_layer.first().expect("top layer should not be empty");
        Ok(Some(hash))
    }

    async fn add_inner(&mut self, mut hash: Hash, finalize: bool) -> Result<()> {
        let max_layer_size = self.args.target_block_size as usize / hash::SIZE;

        for i in 0.. {
            if i >= self.layers.len() {
                let layer = vec![hash];
                self.layers.push(layer);
                break;
            }

            let layer = self.layers.get_mut(i).expect("layer should exist");
            layer.push(hash);
            let len = layer.len();

            if !finalize && len <= max_layer_size {
                break;
            }

            let level = (i + 1).try_into().map_err(|_| Error::TooManyBlockLevels)?;
            let range = if finalize { ..len } else { ..(len - 1) };
            let children = layer.drain(range).collect();
            let block = Block::branch(level, children).await?;
            hash = upload_block(self.args.clone(), self.state.clone(), block).await?;
            self.state.write().await.archive.add_ref(&hash);
        }

        Ok(())
    }
}

async fn upload_block(args: Arc<Args>, state: Arc<RwLock<State>>, block: Block) -> Result<Hash> {
    let key = block.storage_key();
    let hash = block.hash().to_owned();

    if !block_exists(args.clone(), state.clone(), &hash).await {
        let bytes = block.encode(args.compression_level).await?;
        state.write().await.storage.put(&key, bytes).await?;
        state.write().await.stats.blocks_uploaded += 1;
    }

    state.write().await.stats.blocks_used += 1;
    Ok(hash)
}

async fn block_exists(args: Arc<Args>, state: Arc<RwLock<State>>, hash: &Hash) -> bool {
    state.read().await.archive.ref_counts.contains(hash) || args.ref_counts.contains(hash)
}

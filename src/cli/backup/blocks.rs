use std::sync::Arc;

use crate::{
    block::{Block, BlockRecord},
    error::{Error, Result},
    hash::{self, Hash},
    storage,
};

use super::{Args, State};

pub struct UploadTree {
    args: Arc<Args>,
    state: Arc<State>,
    layers: Vec<Vec<Hash>>,
}

impl UploadTree {
    pub fn new(args: Arc<Args>, state: Arc<State>) -> Self {
        UploadTree {
            args,
            state,
            layers: vec![],
        }
    }

    pub async fn add_leaf(&mut self, data: Vec<u8>) -> Result<()> {
        let block = Block::leaf(data).await?;
        let hash = upload_block(self.args.clone(), self.state.clone(), block).await?;
        self.add_inner(hash, false).await?;
        Ok(())
    }

    pub async fn finalize(mut self) -> Result<Option<Hash>> {
        if self.layers.is_empty() {
            return Ok(None);
        }

        let bottom_layer = self.layers.first_mut().unwrap();
        let hash = bottom_layer.pop().unwrap();
        self.add_inner(hash, true).await?;

        let top_layer = self.layers.last().unwrap();
        let hash = *top_layer.first().unwrap();
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

            let layer = self.layers.get_mut(i).unwrap();
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
        }

        Ok(())
    }
}

async fn upload_block(args: Arc<Args>, state: Arc<State>, block: Block) -> Result<Hash> {
    let hash = block.hash().to_owned();
    let semaphore = state.block_locks.write().await.semaphore(&hash);
    let permit = semaphore.acquire().await?;

    if !block_exists(args.clone(), state.clone(), &hash).await {
        let key = storage::block_key(&hash);
        let bytes = block.encode(args.compression_level).await?;
        let size = bytes.len() as u64;

        if !args.dry_run {
            state.storage.write().await.put(&key, bytes).await?;
            state.stats.write().await.blocks_uploaded += 1;
            state.stats.write().await.content_bytes_uploaded += size;
        }

        let record = BlockRecord { ref_count: 1, size };
        state.block_records.write().await.insert(hash, record);
    }

    state.archive.write().await.add_ref(&hash);
    state.stats.write().await.blocks_referenced += 1;

    drop(permit);
    Ok(hash)
}

async fn block_exists(_args: Arc<Args>, state: Arc<State>, hash: &Hash) -> bool {
    state.archive.read().await.block_refs.contains(hash)
        || state.block_records.read().await.contains(hash)
}

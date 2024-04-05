use std::sync::Arc;

use crate::{
    block::{Block, BlockRecord},
    entity::EntityIndex,
    error::{Error, Result},
    hash::{self, Hash},
};

use super::BackupState;

#[derive(Debug)]
pub struct UploadTree {
    state: Arc<BackupState>,
    layers: Vec<Vec<Hash<Block>>>,
}

impl UploadTree {
    pub fn new(state: Arc<BackupState>) -> Self {
        UploadTree {
            state,
            layers: vec![],
        }
    }

    pub async fn add_leaf(&mut self, data: Vec<u8>) -> Result<()> {
        let block = Block::leaf(data).await?;
        let hash = self.upload_block(block).await?;
        self.add_inner(hash, false).await
    }

    pub async fn finalize(mut self) -> Result<Option<Hash<Block>>> {
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

    async fn add_inner(&mut self, mut hash: Hash<Block>, finalize: bool) -> Result<()> {
        let max_layer_size = self.state.target_block_size as usize / hash::SIZE;

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
            hash = self.upload_block(block).await?;
        }

        Ok(())
    }

    async fn upload_block(&mut self, block: Block) -> Result<Hash<Block>> {
        let hash = block.hash().to_owned();
        let lock = self.state.block_locks.write().await.lock(&hash);
        let permit = lock.acquire().await?;

        let block_exists = self.state.block_records.read().await.contains(&hash);
        if block_exists {
            let mut block_records = self.state.block_records.write().await;
            let record = block_records.get_mut(&hash).unwrap();
            record.ref_count += 1;
        } else {
            let key = hash.key();
            let bytes = block.encode(self.state.compression_level).await?;
            let size = bytes.len() as u64;

            if !self.state.dry_run {
                self.state.storage.put(&key, bytes).await?;
                self.state.stats.write().await.blocks_uploaded += 1;
                self.state.stats.write().await.content_bytes_uploaded += size;
            }

            let record = BlockRecord { ref_count: 1, size };
            self.state.block_records.write().await.insert(hash, record);
        }

        self.state.archive.write().await.add_ref(&hash);
        self.state.stats.write().await.blocks_referenced += 1;

        drop(permit);
        Ok(hash)
    }
}

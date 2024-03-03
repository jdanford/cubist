use std::{pin::pin, sync::Arc};

use tokio::fs::File;
use tokio_stream::StreamExt;

use crate::{
    backup::BackupArgs,
    block::{self, Block},
    error::Result,
    hash::{self, Hash},
};

pub async fn upload_file(args: Arc<BackupArgs>, file: &mut File) -> Result<Option<Hash>> {
    let mut chunker = block::chunker(file, args.target_block_size);
    let mut chunks = pin!(chunker.as_stream());
    let mut tree = UploadTree::new(args);

    while let Some(chunk_result) = chunks.next().await {
        let chunk = chunk_result?;
        tree.add_leaf(chunk.data).await?;
    }

    let hash = tree.finalize().await?;
    Ok(hash)
}

pub struct UploadTree {
    args: Arc<BackupArgs>,
    layers: Vec<Vec<Hash>>,
}

impl UploadTree {
    pub fn new(args: Arc<BackupArgs>) -> Self {
        UploadTree {
            args,
            layers: vec![],
        }
    }

    pub async fn add_leaf(&mut self, data: Vec<u8>) -> Result<()> {
        let block = Block::leaf(data).await?;
        let hash = upload_block(self.args.clone(), block).await?;
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

            let level = (i + 1).try_into().unwrap();
            let range = if finalize { ..len } else { ..(len - 1) };
            let children = layer.drain(range).collect();
            let block = Block::branch(level, children).await?;
            hash = upload_block(self.args.clone(), block).await?;
        }

        Ok(())
    }
}

async fn upload_block(args: Arc<BackupArgs>, block: Block) -> Result<Hash> {
    let key = block.storage_key();
    let hash = block.hash().to_owned();

    if !args.storage.exists(&key).await? {
        let bytes = block.encode(args.compression_level).await?;
        args.storage.put(&key, bytes).await?;
    }

    Ok(hash)
}

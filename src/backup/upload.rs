use std::{pin::pin, sync::Arc};

use tokio::{fs::File, io::AsyncWriteExt};
use tokio_stream::StreamExt;

use crate::{
    backup::BackupArgs,
    block,
    error::Result,
    hash::{self, Hash},
};

pub async fn upload_file(args: Arc<BackupArgs>, file: &mut File) -> Result<Option<Hash>> {
    let mut chunker = block::chunker(file, args.target_block_size);
    let mut chunks = pin!(chunker.as_stream());

    let max_layer_size = args.target_block_size as usize / hash::SIZE;
    let mut tree = UploadTree::new(args, max_layer_size);

    while let Some(chunk_result) = chunks.next().await {
        let chunk = chunk_result?;
        tree.add(&chunk.data).await?;
    }

    let hash = tree.finalize().await?;
    Ok(hash)
}

pub struct UploadTree {
    args: Arc<BackupArgs>,
    max_layer_size: usize,
    layers: Vec<Vec<Hash>>,
}

impl UploadTree {
    pub fn new(args: Arc<BackupArgs>, max_layer_size: usize) -> Self {
        UploadTree {
            args,
            max_layer_size,
            layers: vec![],
        }
    }

    pub async fn add(&mut self, data: &[u8]) -> Result<()> {
        let hash = upload_leaf_block(self.args.clone(), data).await?;
        self.add_leaf(hash, false).await?;
        Ok(())
    }

    pub async fn finalize(mut self) -> Result<Option<Hash>> {
        if self.layers.is_empty() {
            return Ok(None);
        }

        let bottom_layer = self.layers.first_mut().unwrap();
        let hash = bottom_layer.pop().unwrap();
        self.add_leaf(hash, true).await?;

        let top_layer = self.layers.last().unwrap();
        let hash = *top_layer.first().unwrap();
        Ok(Some(hash))
    }

    async fn add_leaf(&mut self, mut hash: Hash, last: bool) -> Result<()> {
        for i in 0.. {
            if i >= self.layers.len() {
                let layer = vec![hash];
                self.layers.push(layer);
                break;
            }

            let layer = self.layers.get_mut(i).unwrap();
            layer.push(hash);
            let len = layer.len();

            if !last && len <= self.max_layer_size {
                break;
            }

            let level = (i + 1).try_into().unwrap();
            let range = if last { ..len } else { ..(len - 1) };
            let hashes = layer.drain(range);
            hash = upload_branch_block(self.args.clone(), level, hashes).await?;
        }

        Ok(())
    }
}

async fn upload_branch_block<I>(args: Arc<BackupArgs>, level: u8, hashes: I) -> Result<Hash>
where
    I: Iterator<Item = Hash>,
{
    let data = concat_hashes(hashes);
    let hash = block::hash(&data).await?;
    let key = block::key(&hash);

    if !args.storage.exists(&key).await? {
        upload_block(args, level, &key, &data).await?;
    }

    Ok(hash)
}

async fn upload_leaf_block(args: Arc<BackupArgs>, data: &[u8]) -> Result<Hash> {
    let hash = block::hash(data).await?;
    let key = block::key(&hash);

    if !args.storage.exists(&key).await? {
        let compressed_data = block::compress(data, args.compression_level).await?;
        upload_block(args, 0, &key, &compressed_data).await?;
    }

    Ok(hash)
}

async fn upload_block(args: Arc<BackupArgs>, level: u8, key: &str, data: &[u8]) -> Result<()> {
    let mut bytes = vec![];
    bytes.write_u8(level).await?;
    bytes.write_all(data).await?;
    args.storage.put(key, bytes).await?;
    Ok(())
}

fn concat_hashes<I>(hashes: I) -> Vec<u8>
where
    I: Iterator<Item = Hash>,
{
    hashes
        .into_iter()
        .flat_map(|hash| *hash.as_bytes())
        .collect()
}

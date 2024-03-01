use std::{fmt, sync::Arc};

use tokio::io::AsyncWriteExt;

use crate::{
    backup::BackupArgs, block, error::Result, hash::Hash
};

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

    pub async fn finalize(mut self) -> Result<Hash> {
        let bottom_layer = self.layers.first_mut().unwrap();
        let hash = bottom_layer.pop().unwrap();
        self.add_leaf(hash, true).await?;

        let top_layer = self.layers.last().unwrap();
        let hash = *top_layer.first().unwrap();
        Ok(hash)
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

impl fmt::Display for UploadTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, layer) in self.layers.iter().rev().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }

            write!(f, "[")?;
            for (i, hash) in layer.iter().enumerate() {
                if i > 0 {
                    write!(f, " ")?;
                }

                let minihash = &hash.as_bytes()[..2];
                for byte in minihash {
                    write!(f, "{byte:02x}")?;
                }
            }
            write!(f, "]")?;
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
    let key = block_key(&hash);

    if !args.storage.exists(&args.bucket, &key).await? {
        upload_block(args, level, &key, &data).await?;
    }

    Ok(hash)
}

async fn upload_leaf_block(args: Arc<BackupArgs>, data: &[u8]) -> Result<Hash> {
    let hash = block::hash(data).await?;
    let key = block_key(&hash);

    if !args.storage.exists(&args.bucket, &key).await? {
        let compressed_data = block::compress(data, args.compression_level).await?;
        upload_block(args, 0, &key, &compressed_data).await?;
    }

    Ok(hash)
}

async fn upload_block(args: Arc<BackupArgs>, level: u8, key: &str, data: &[u8]) -> Result<()> {
    let mut bytes = vec![];
    bytes.write_u8(level).await?;
    bytes.write_all(data).await?;
    args.storage.put(&args.bucket, key, bytes).await?;
    Ok(())
}

fn block_key(hash: &Hash) -> String {
    format!("block:{hash}")
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

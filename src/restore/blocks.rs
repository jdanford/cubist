use std::{
    io::SeekFrom,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

use async_recursion::async_recursion;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    block::{self, Block},
    error::{Error, Result},
    hash::Hash,
    restore::{LocalBlock, RestoreArgs, RestoreState},
};

use super::files::PendingDownload;

pub struct ActiveDownload {
    file: File,
    inode: u64,
    offset: u64,
}

impl ActiveDownload {
    pub async fn new(pending_file: &PendingDownload) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&pending_file.path)
            .await?;

        Ok(ActiveDownload {
            file,
            inode: pending_file.metadata.inode,
            offset: 0,
        })
    }
}

impl Deref for ActiveDownload {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for ActiveDownload {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

#[async_recursion]
pub async fn download_blocks(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    file: &mut ActiveDownload,
    hash: Hash,
    expected_level: Option<u8>,
) -> Result<()> {
    let maybe_block = state.lock().unwrap().local_blocks.get(&hash).copied();
    if let Some(local_block) = maybe_block {
        block::assert_level(0, expected_level)?;
        let data = read_local_block(args, local_block).await?;
        write_leaf_block(file, &data).await?;
        return Ok(());
    }

    let key = Block::storage_key_for_hash(&hash);
    let bytes = args.storage.get(&key).await?;
    let block = Block::decode(hash, expected_level, &bytes).await?;
    match block {
        Block::Leaf { data, .. } => {
            let local_block = write_leaf_block(file, &data).await?;
            state.lock().unwrap().local_blocks.insert(hash, local_block);
        }
        Block::Branch {
            level, children, ..
        } => {
            for hash in children {
                download_blocks(args.clone(), state.clone(), file, hash, Some(level - 1)).await?;
            }
        }
    }

    Ok(())
}

async fn write_leaf_block(file: &mut ActiveDownload, data: &[u8]) -> Result<LocalBlock> {
    let length = data.len().try_into().expect("catastrophically large block");
    let local_block = LocalBlock::new(file.inode, file.offset, length);

    file.write_all(data).await?;
    file.offset += u64::from(length);

    Ok(local_block)
}

async fn read_local_block(args: Arc<RestoreArgs>, local_block: LocalBlock) -> Result<Vec<u8>> {
    let path = args
        .archive
        .path(local_block.inode)
        .ok_or(Error::NoPathForInode(local_block.inode))?;
    let mut block_file = File::open(path).await?;
    let seek_pos = SeekFrom::Start(local_block.offset);
    let read_length = local_block.length as usize;
    let mut data = vec![0; read_length];

    block_file.seek(seek_pos).await?;
    block_file.read_exact(&mut data).await?;
    Ok(data)
}
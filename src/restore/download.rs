use std::{
    io::SeekFrom,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_recursion::async_recursion;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    block,
    error::{Error, Result},
    file::Metadata,
    hash::{self, Hash},
    restore::{LocalBlock, RestoreArgs, RestoreState},
};

pub struct PendingDownload {
    pub metadata: Metadata,
    pub hash: Option<Hash>,
    pub path: PathBuf,
}

pub struct ActiveDownload {
    file: File,
    inode: u64,
    offset: u64,
}

impl ActiveDownload {
    pub async fn new(pending: &PendingDownload) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&pending.path)
            .await?;

        Ok(ActiveDownload {
            file,
            inode: pending.metadata.inode,
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
        let data = read_local_block(args, local_block).await?;
        write_leaf_block(file, &data).await?;
        return Ok(());
    }

    let key = block::key(&hash);
    let block = args.storage.get(&key).await?;
    let (&level, data) = block.split_first().unwrap();
    assert_eq!(level, expected_level.unwrap_or(level));

    if level == 0 {
        let data = block::decompress_and_verify(&hash, data).await?;
        let local_block = write_leaf_block(file, &data).await?;
        state.lock().unwrap().local_blocks.insert(hash, local_block);
        return Ok(());
    }

    let hashes = hash::split(data);
    for hash in hashes {
        download_blocks(
            args.clone(),
            state.clone(),
            file,
            hash,
            Some(level - 1),
        )
        .await?;
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

use std::{
    io::SeekFrom,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use async_recursion::async_recursion;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::{
    block::Block,
    error::{assert_block_level_eq, Error, Result},
    hash::Hash,
    keys,
};

use super::{files::PendingDownload, DownloadArgs, DownloadState};

#[derive(Debug, Clone, Copy)]
pub struct LocalBlock {
    pub inode: u64,
    pub offset: u64,
    pub size: u32,
}

impl LocalBlock {
    pub fn new(inode: u64, offset: u64, size: u32) -> Self {
        LocalBlock {
            inode,
            offset,
            size,
        }
    }
}

#[derive(Debug)]
pub struct ActiveDownload {
    writer: BufWriter<File>,
    inode: u64,
    offset: u64,
}

impl ActiveDownload {
    pub async fn new(pending_file: &PendingDownload) -> Result<Self> {
        let file = File::create(&pending_file.path).await?;
        let writer = BufWriter::new(file);

        Ok(ActiveDownload {
            writer,
            inode: pending_file.metadata.inode,
            offset: 0,
        })
    }
}

impl Deref for ActiveDownload {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        self.writer.get_ref()
    }
}

impl DerefMut for ActiveDownload {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.writer.get_mut()
    }
}

#[async_recursion]
pub async fn download_block_recursive(
    args: Arc<DownloadArgs>,
    state: Arc<DownloadState>,
    file: &mut ActiveDownload,
    hash: Hash,
    level: Option<u8>,
) -> Result<u64> {
    state.stats.write().await.blocks_referenced += 1;

    let lock = state.block_locks.write().await.lock(&hash);
    let permit = lock.acquire().await?;

    // copied to avoid holding lock
    let maybe_block = state.local_blocks.read().await.get(&hash).copied();
    if let Some(local_block) = maybe_block {
        assert_block_level_eq(hash, 0, level)?;
        let data = read_local_block(args, local_block).await?;
        write_local_block(state.clone(), file, &data).await?;
        let size = file.offset;
        return Ok(size);
    }

    let key = keys::block(&hash);
    let bytes = state.storage.get(&key).await?;
    state.stats.write().await.blocks_downloaded += 1;
    state.stats.write().await.content_bytes_downloaded += bytes.len() as u64;

    let block = Block::decode(&hash, level, &bytes).await?;
    match block {
        Block::Leaf { data, .. } => {
            let local_block = write_local_block(state.clone(), file, &data).await?;
            state.local_blocks.write().await.insert(hash, local_block);
        }
        Block::Branch {
            level, children, ..
        } => {
            for hash in children {
                download_block_recursive(args.clone(), state.clone(), file, hash, Some(level - 1))
                    .await?;
            }
        }
    }

    drop(permit);
    let size = file.offset;
    Ok(size)
}

async fn write_local_block(
    state: Arc<DownloadState>,
    file: &mut ActiveDownload,
    data: &[u8],
) -> Result<LocalBlock> {
    let size = data.len() as u64;
    let safe_size = size.try_into().map_err(|_| Error::InvalidBlockSize(size))?;
    let local_block = LocalBlock::new(file.inode, file.offset, safe_size);

    file.write_all(data).await?;
    file.offset += size;
    state.stats.write().await.bytes_written += size;

    Ok(local_block)
}

async fn read_local_block(args: Arc<DownloadArgs>, local_block: LocalBlock) -> Result<Vec<u8>> {
    let path = args
        .archive
        .path(local_block.inode)
        .ok_or_else(|| Error::InodeDoesNotExist(local_block.inode))?;
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);

    let seek_pos = SeekFrom::Start(local_block.offset);
    let buffer_size = local_block.size as usize;
    let mut data = vec![0; buffer_size];

    reader.seek(seek_pos).await?;
    reader.read_exact(&mut data).await?;

    Ok(data)
}

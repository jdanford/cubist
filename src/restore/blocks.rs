use std::{
    io::SeekFrom,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

use async_recursion::async_recursion;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use crate::{
    block::Block,
    error::{assert_block_level_eq, Error, Result},
    hash::Hash,
    storage,
};

use super::{files::PendingDownload, Args, State};

#[derive(Debug, Clone, Copy)]
pub struct LocalBlock {
    pub inode: u64,
    pub offset: u64,
    pub length: u32,
}

impl LocalBlock {
    pub fn new(inode: u64, offset: u64, length: u32) -> Self {
        LocalBlock {
            inode,
            offset,
            length,
        }
    }
}

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
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    file: &mut ActiveDownload,
    hash: Hash,
    level: Option<u8>,
) -> Result<()> {
    state.lock().unwrap().stats.blocks_used += 1;

    // copied to avoid holding mutex lock
    let maybe_block = state.lock().unwrap().local_blocks.get(&hash).copied();
    if let Some(local_block) = maybe_block {
        assert_block_level_eq(hash, 0, level)?;
        let data = read_local_block(args, local_block).await?;
        write_local_block(state.clone(), file, &data).await?;
        return Ok(());
    }

    let key = storage::block_key(&hash);
    let bytes = args.storage.get(&key).await?;
    state.lock().unwrap().stats.blocks_downloaded += 1;
    state.lock().unwrap().stats.bytes_downloaded += bytes.len() as u64;

    let block = Block::decode(hash, level, &bytes).await?;
    match block {
        Block::Leaf { data, .. } => {
            let local_block = write_local_block(state.clone(), file, &data).await?;
            state.lock().unwrap().local_blocks.insert(hash, local_block);
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

    Ok(())
}

async fn write_local_block(
    state: Arc<Mutex<State>>,
    file: &mut ActiveDownload,
    data: &[u8],
) -> Result<LocalBlock> {
    let length = data.len();
    let safe_length = length
        .try_into()
        .map_err(|_| Error::InvalidBlockSize(length))?;
    let local_block = LocalBlock::new(file.inode, file.offset, safe_length);

    file.write_all(data).await?;
    file.offset += length as u64;

    state.lock().unwrap().stats.bytes_written += length as u64;
    Ok(local_block)
}

async fn read_local_block(args: Arc<Args>, local_block: LocalBlock) -> Result<Vec<u8>> {
    let path = args
        .archive
        .path(local_block.inode)
        .ok_or_else(|| Error::InodeDoesNotExist(local_block.inode))?;
    let mut block_file = File::open(path).await?;
    let seek_pos = SeekFrom::Start(local_block.offset);
    let read_length = local_block.length as usize;
    let mut data = vec![0; read_length];

    block_file.seek(seek_pos).await?;
    block_file.read_exact(&mut data).await?;

    Ok(data)
}

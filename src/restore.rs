use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
};

use tokio::{
    fs::{create_dir, symlink, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    archive::Archive,
    block::{decompress_and_verify, BlockHash},
    cloud::Cloud,
    error::Error,
    file::{FileHash, Node},
};

struct RestoreArgs {
    cloud: Cloud,
    bucket: String,
    output_path: PathBuf,
    archive: Archive,
}

struct RestoreState {
    local_blocks: HashMap<BlockHash, LocalBlock>,
}

struct LocalBlock {
    inode: u64,
    offset: u64,
    length: u32,
}

impl LocalBlock {
    fn new(inode: u64, offset: u64, length: u32) -> Self {
        LocalBlock {
            inode,
            offset,
            length,
        }
    }
}

pub async fn restore(cloud: Cloud, bucket: String, output_path: PathBuf) -> Result<(), Error> {
    // TODO: download archive
    let archive = Archive::new();
    let local_blocks = HashMap::new();

    let args = RestoreArgs {
        cloud,
        bucket,
        output_path,
        archive,
    };

    let mut state = RestoreState { local_blocks };

    for (path, node) in args.archive.walk() {
        let path = args.output_path.join(path);
        restore_from_node(&args, &mut state, &path, node).await?;
    }

    Ok(())
}

async fn restore_from_node(
    args: &RestoreArgs,
    state: &mut RestoreState,
    path: &Path,
    node: &Node,
) -> Result<(), Error> {
    match node {
        Node::File { hash, .. } => {
            download_from_hash(args, state, node, path, hash).await?;
        }
        Node::Symlink { path: src, .. } => {
            symlink(src, path).await?;
        }
        Node::Directory { .. } => {
            create_dir(path).await?;
        }
    }

    // restore_metadata()

    Ok(())
}

async fn download_from_hash(
    args: &RestoreArgs,
    state: &mut RestoreState,
    node: &Node,
    path: &Path,
    hash: &FileHash,
) -> Result<(), Error> {
    let key = hash.to_string();
    let packed_block_hashes = args.cloud.get(&args.bucket, &key).await?;
    let block_hashes = packed_block_hashes
        .chunks_exact(32)
        .map(|bytes| BlockHash::from_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();

    let mut file = OpenOptions::new().write(true).open(path).await?;
    download_blocks(args, state, node, &mut file, &block_hashes).await?;
    Ok(())
}

async fn download_blocks(
    args: &RestoreArgs,
    state: &mut RestoreState,
    node: &Node,
    file: &mut File,
    block_hashes: &[BlockHash],
) -> Result<(), Error> {
    let mut offset = 0;
    for hash in block_hashes {
        let compressed_data = download_block(args, state, hash).await?;
        let data = decompress_and_verify(hash, &compressed_data).await?;
        file.write_all(&data).await?;

        let length = data.len().try_into().expect("catastrophically large block");
        state
            .local_blocks
            .entry(*hash)
            .or_insert_with(|| LocalBlock::new(node.metadata().inode, offset, length));

        offset += length as u64;
    }

    file.sync_all().await?;
    Ok(())
}

async fn download_block(
    args: &RestoreArgs,
    state: &mut RestoreState,
    hash: &BlockHash,
) -> Result<Vec<u8>, Error> {
    if let Some(local_block) = state.local_blocks.get(hash) {
        read_local_block(args, local_block).await
    } else {
        download_remote_block(args, hash).await
    }
}

async fn read_local_block(args: &RestoreArgs, local_block: &LocalBlock) -> Result<Vec<u8>, Error> {
    let path = args
        .archive
        .path(local_block.inode)
        .ok_or_else(|| Error::NoPathForInode(local_block.inode))?;
    let mut block_file = File::open(path).await?;
    let seek_pos = SeekFrom::Start(local_block.offset);
    let read_length = local_block.length as usize;
    let mut data = vec![0; read_length];

    block_file.seek(seek_pos).await?;
    block_file.read_exact(&mut data).await?;
    Ok(data)
}

async fn download_remote_block(args: &RestoreArgs, hash: &BlockHash) -> Result<Vec<u8>, Error> {
    let key = hash.to_string();
    let data = args.cloud.get(&args.bucket, &key).await?;
    Ok(data)
}

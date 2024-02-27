use std::{
    collections::HashMap,
    fs::Permissions,
    io::{Cursor, SeekFrom},
    os::unix::fs::{chown, lchown, PermissionsExt},
    path::{Path, PathBuf},
};

use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    task::spawn_blocking,
};

use crate::{
    block::{self, BlockHash},
    error::{Error, Result},
    file::{try_exists, Archive, FileHash, Node},
    storage::Storage,
};

struct RestoreArgs<S> {
    storage: S,
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

pub async fn restore<S: Storage>(storage: S, bucket: String, output_path: PathBuf) -> Result<()> {
    let archive = download_archive(&storage, &bucket).await?;
    let local_blocks = HashMap::new();

    let args = RestoreArgs {
        storage,
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

async fn download_archive<S: Storage>(storage: &S, bucket: &str) -> Result<Archive> {
    let latest_key = "archive:latest";
    let timestamp_bytes = storage.get(bucket, latest_key).await?;
    let timestamp = String::from_utf8(timestamp_bytes)?;

    let key = format!("archive:{}", timestamp);
    let serialized_archive = storage.get(bucket, &key).await?;

    let reader = Cursor::new(serialized_archive);
    let archive = spawn_blocking(move || ciborium::from_reader(reader)).await??;
    Ok(archive)
}

async fn restore_from_node<S: Storage>(
    args: &RestoreArgs<S>,
    state: &mut RestoreState,
    path: &Path,
    node: &Node,
) -> Result<()> {
    if try_exists(path).await? {
        return Err(Error::FileAlreadyExists(path.to_owned()));
    }

    match node {
        Node::File { hash, .. } => {
            download_from_hash(args, state, node, path, hash).await?;
        }
        Node::Symlink { path: src, .. } => {
            fs::symlink(src, path).await?;
            restore_metadata(path, node).await?;
        }
        Node::Directory { .. } => {
            fs::create_dir(path).await?;
            restore_metadata(path, node).await?;
        }
    }

    Ok(())
}

async fn restore_metadata(path: &Path, node: &Node) -> Result<()> {
    let owner = node.metadata().owner;
    let group = node.metadata().group;
    let mode = node.metadata().mode;

    if node.is_symlink() {
        lchown(path, Some(owner), Some(group))?;
    } else {
        chown(path, Some(owner), Some(group))?;
    }

    let permissions = Permissions::from_mode(mode);
    fs::set_permissions(path, permissions).await?;
    Ok(())
}

async fn download_from_hash<S: Storage>(
    args: &RestoreArgs<S>,
    state: &mut RestoreState,
    node: &Node,
    path: &Path,
    hash: &FileHash,
) -> Result<()> {
    let key = hash.key();
    let packed_block_hashes = args.storage.get(&args.bucket, &key).await?;
    let block_hashes = packed_block_hashes
        .chunks_exact(32)
        .map(|bytes| BlockHash::from_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();

    let mut file = OpenOptions::new().write(true).open(path).await?;
    download_blocks(args, state, node, &mut file, &block_hashes).await?;
    restore_metadata(path, node).await?;
    Ok(())
}

async fn download_blocks<S: Storage>(
    args: &RestoreArgs<S>,
    state: &mut RestoreState,
    node: &Node,
    file: &mut File,
    block_hashes: &[BlockHash],
) -> Result<()> {
    let mut offset = 0;
    for hash in block_hashes {
        let compressed_data = download_block(args, state, hash).await?;
        let data = block::decompress_and_verify(hash, &compressed_data).await?;
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

async fn download_block<S: Storage>(
    args: &RestoreArgs<S>,
    state: &mut RestoreState,
    hash: &BlockHash,
) -> Result<Vec<u8>> {
    if let Some(local_block) = state.local_blocks.get(hash) {
        read_local_block(args, local_block).await
    } else {
        download_remote_block(args, hash).await
    }
}

async fn read_local_block<S: Storage>(
    args: &RestoreArgs<S>,
    local_block: &LocalBlock,
) -> Result<Vec<u8>> {
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

async fn download_remote_block<S: Storage>(
    args: &RestoreArgs<S>,
    hash: &BlockHash,
) -> Result<Vec<u8>> {
    let key = hash.key();
    let data = args.storage.get(&args.bucket, &key).await?;
    Ok(data)
}

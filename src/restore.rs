use std::{
    collections::HashMap,
    fs::Permissions,
    io::{Cursor, SeekFrom},
    os::unix::fs::{chown, lchown, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    spawn,
    sync::Semaphore,
    task::spawn_blocking,
};

use crate::{
    block::{self, BlockHash},
    error::{Error, Result},
    file::{try_exists, Archive, FileHash, FileType, Metadata, Node},
    hash,
    storage::{LocalStorage, Storage},
};

struct RestoreArgs {
    storage: LocalStorage,
    max_concurrency: usize,
    bucket: String,
    output_path: PathBuf,
    archive: Archive,
}

struct RestoreState {
    local_blocks: HashMap<BlockHash, LocalBlock>,
}

struct PendingFile {
    metadata: Metadata,
    hash: FileHash,
    path: PathBuf,
}

#[derive(Clone, Copy)]
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

pub async fn restore(
    storage: LocalStorage,
    max_concurrency: usize,
    bucket: String,
    output_path: PathBuf,
) -> Result<()> {
    let archive = download_archive(&storage, &bucket).await?;
    let local_blocks = HashMap::new();
    let args = Arc::new(RestoreArgs {
        storage,
        max_concurrency,
        bucket,
        output_path,
        archive,
    });
    let state = Arc::new(Mutex::new(RestoreState { local_blocks }));
    let (sender, receiver) = async_channel::bounded(args.max_concurrency);

    let downloader_args = args.clone();
    let downloader_state = state.clone();
    let downloader_task = spawn(async move {
        download_pending_files(
            downloader_args.clone(),
            downloader_state.clone(),
            receiver.clone(),
        )
        .await;
    });

    restore_recursive(args.clone(), state.clone(), sender.clone()).await?;
    downloader_task.await?;
    Ok(())
}

async fn download_archive(storage: &LocalStorage, bucket: &str) -> Result<Archive> {
    let latest_key = "archive:latest";
    let timestamp_bytes = storage.get(bucket, latest_key).await?;
    let timestamp = String::from_utf8(timestamp_bytes)?;

    let key = format!("archive:{timestamp}");
    let serialized_archive = storage.get(bucket, &key).await?;

    let reader = Cursor::new(serialized_archive);
    let archive = spawn_blocking(move || ciborium::from_reader(reader)).await??;
    Ok(archive)
}

async fn restore_recursive(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    sender: Sender<PendingFile>,
) -> Result<()> {
    for (path, node) in args.archive.walk() {
        let path = args.output_path.join(path);
        restore_from_node(args.clone(), state.clone(), sender.clone(), &path, node).await?;
    }

    Ok(())
}

async fn restore_from_node(
    _args: Arc<RestoreArgs>,
    _state: Arc<Mutex<RestoreState>>,
    sender: Sender<PendingFile>,
    path: &Path,
    node: &Node,
) -> Result<()> {
    if try_exists(path).await? {
        return Err(Error::FileAlreadyExists(path.to_owned()));
    }

    match node {
        Node::File { metadata, hash } => {
            let pending_file = PendingFile {
                metadata: metadata.clone(),
                hash: *hash,
                path: path.to_owned(),
            };
            sender.send(pending_file).await?;
        }
        Node::Symlink { path: src, .. } => {
            fs::symlink(src, path).await?;
            restore_metadata_from_node(path, node).await?;
        }
        Node::Directory { .. } => {
            fs::create_dir(path).await?;
            restore_metadata_from_node(path, node).await?;
        }
    }

    Ok(())
}

async fn restore_metadata_from_node(path: &Path, node: &Node) -> Result<()> {
    restore_metadata(path, node.metadata(), node.file_type()).await
}

async fn restore_metadata(path: &Path, metadata: &Metadata, file_type: FileType) -> Result<()> {
    let owner = metadata.owner;
    let group = metadata.group;
    let mode = metadata.mode;

    if file_type.is_symlink() {
        lchown(path, Some(owner), Some(group))?;
    } else {
        chown(path, Some(owner), Some(group))?;
    }

    let permissions = Permissions::from_mode(mode);
    fs::set_permissions(path, permissions).await?;
    Ok(())
}

async fn download_pending_files(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    receiver: Receiver<PendingFile>,
) {
    let semaphore = Arc::new(Semaphore::new(args.max_concurrency));
    while let Ok(pending_file) = receiver.recv().await {
        let args = args.clone();
        let state = state.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        spawn(async move {
            download_pending_file(args, state, pending_file)
                .await
                .unwrap();
            drop(permit);
        });
    }
}

async fn download_pending_file(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    pending_file: PendingFile,
) -> Result<()> {
    let key = pending_file.hash.key();
    let packed_block_hashes = args.storage.get(&args.bucket, &key).await?;
    let block_hashes = packed_block_hashes
        .chunks_exact(hash::SIZE)
        .map(|bytes| BlockHash::from_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&pending_file.path)
        .await?;
    download_blocks(
        args,
        state,
        &pending_file.metadata,
        &mut file,
        &block_hashes,
    )
    .await?;
    restore_metadata(&pending_file.path, &pending_file.metadata, FileType::File).await?;
    Ok(())
}

async fn download_blocks(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    metadata: &Metadata,
    file: &mut File,
    block_hashes: &[BlockHash],
) -> Result<()> {
    let mut offset = 0;
    for hash in block_hashes {
        let (compressed_data, is_new) = download_block(args.clone(), state.clone(), hash).await?;
        let data = block::decompress_and_verify(hash, &compressed_data).await?;
        let length = data.len().try_into().expect("catastrophically large block");
        file.write_all(&data).await?;

        if is_new {
            let local_block = LocalBlock::new(metadata.inode, offset, length);
            state
                .lock()
                .unwrap()
                .local_blocks
                .insert(hash.to_owned(), local_block);
        }

        offset += u64::from(length);
    }

    file.sync_all().await?;
    Ok(())
}

async fn download_block(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    hash: &BlockHash,
) -> Result<(Vec<u8>, bool)> {
    let maybe_block = state.lock().unwrap().local_blocks.get(hash).copied();
    if let Some(local_block) = maybe_block {
        let data = read_local_block(args, local_block).await?;
        Ok((data, false))
    } else {
        let data = download_remote_block(args, hash).await?;
        Ok((data, true))
    }
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

async fn download_remote_block(args: Arc<RestoreArgs>, hash: &BlockHash) -> Result<Vec<u8>> {
    let key = hash.key();
    let data = args.storage.get(&args.bucket, &key).await?;
    Ok(data)
}

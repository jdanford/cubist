mod download;

use std::{
    collections::HashMap,
    fs::Permissions,
    os::unix::fs::{chown, lchown, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use tokio::{fs, spawn, sync::Semaphore, task::spawn_blocking};

use crate::{
    error::{Error, Result},
    file::{try_exists, Archive, FileType, Metadata, Node},
    hash::Hash,
    storage::BoxedStorage,
};

use self::download::{download_block, ActiveDownload, PendingDownload};

pub struct RestoreArgs {
    pub storage: BoxedStorage,
    pub max_concurrency: usize,
    pub bucket: String,
    pub output_path: PathBuf,
    pub archive: Archive,
}

pub struct RestoreState {
    pub local_blocks: HashMap<Hash, LocalBlock>,
}

#[derive(Clone, Copy)]
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

pub async fn restore(
    storage: BoxedStorage,
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
        download_pending_files(downloader_args, downloader_state, receiver).await;
    });

    restore_recursive(args, state, sender).await?;
    downloader_task.await?;
    Ok(())
}

async fn download_archive(storage: &BoxedStorage, bucket: &str) -> Result<Archive> {
    let latest_key = "archive:latest";
    let timestamp_bytes = storage.get(bucket, latest_key).await?;
    let timestamp = String::from_utf8(timestamp_bytes)?;

    let key = format!("archive:{timestamp}");
    let serialized_archive = storage.get(bucket, &key).await?;
    let archive = spawn_blocking(move || bincode::deserialize(&serialized_archive)).await??;
    Ok(archive)
}

async fn restore_recursive(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    sender: Sender<PendingDownload>,
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
    sender: Sender<PendingDownload>,
    path: &Path,
    node: &Node,
) -> Result<()> {
    if try_exists(path).await? {
        return Err(Error::FileAlreadyExists(path.to_owned()));
    }

    match node {
        Node::File { metadata, hash } => {
            let pending_file = PendingDownload {
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

async fn download_pending_files(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    receiver: Receiver<PendingDownload>,
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
    pending_file: PendingDownload,
) -> Result<()> {
    let mut file = ActiveDownload::open(&pending_file).await?;
    download_block(args, state, &mut file, pending_file.hash, None).await?;
    file.sync_all().await?;
    restore_metadata(&pending_file.path, &pending_file.metadata, FileType::File).await?;
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

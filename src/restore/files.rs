use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use log::info;
use tokio::{fs, spawn, sync::Semaphore, task::spawn_blocking};

use crate::{
    archive::Archive,
    error::{Error, Result},
    file::{restore_metadata, restore_metadata_from_node, try_exists, FileType, Metadata, Node},
    hash::{self, Hash},
    restore::blocks::{download_blocks, ActiveDownload},
    serde::deserialize,
    storage::{self, BoxedStorage, ARCHIVE_KEY_LATEST},
};

use super::{RestoreArgs, RestoreState};

pub struct PendingDownload {
    pub metadata: Metadata,
    pub hash: Option<Hash>,
    pub path: PathBuf,
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

pub async fn download_archive(storage: &BoxedStorage) -> Result<Archive> {
    let timestamp_bytes = storage.get(ARCHIVE_KEY_LATEST).await?;
    let timestamp = String::from_utf8(timestamp_bytes)?;
    let key = storage::archive_key(&timestamp);
    let bytes = storage.get(&key).await?;
    let archive = spawn_blocking(move || deserialize(bytes)).await??;
    Ok(archive)
}

pub async fn restore_recursive(
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

pub async fn download_pending_files(
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

    let permit_count = u32::try_from(args.max_concurrency).unwrap();
    let _ = semaphore.acquire_many(permit_count).await.unwrap();
}

async fn download_pending_file(
    args: Arc<RestoreArgs>,
    state: Arc<Mutex<RestoreState>>,
    pending_file: PendingDownload,
) -> Result<()> {
    let mut file = ActiveDownload::new(&pending_file).await?;

    if let Some(hash) = pending_file.hash {
        download_blocks(args, state, &mut file, hash, None).await?;
        file.sync_all().await?;
    }

    restore_metadata(&pending_file.path, &pending_file.metadata, FileType::File).await?;
    let hash_str = hash::format(&pending_file.hash);
    info!("{hash_str} -> {}", pending_file.path.display());
    Ok(())
}

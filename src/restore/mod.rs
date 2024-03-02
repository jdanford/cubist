mod download;
mod main;

pub use self::main::main;

use std::{
    collections::HashMap,
    fs::Permissions,
    io::Cursor,
    os::unix::fs::{chown, lchown, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use log::info;
use tokio::{fs, spawn, sync::Semaphore, task::spawn_blocking};

use crate::{
    archive::Archive,
    error::{Error, Result},
    file::{try_exists, FileType, Metadata, Node},
    hash::{self, Hash},
    storage::BoxedStorage,
};

use self::download::{download_blocks, ActiveDownload, PendingDownload};

pub struct RestoreArgs {
    pub storage: BoxedStorage,
    pub max_concurrency: usize,
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

async fn download_archive(storage: &BoxedStorage) -> Result<Archive> {
    let latest_key = "archive:latest";
    let timestamp_bytes = storage.get(latest_key).await?;
    let timestamp = String::from_utf8(timestamp_bytes)?;

    let key = format!("archive:{timestamp}");
    let serialized_archive = storage.get(&key).await?;
    let reader = Cursor::new(serialized_archive);
    let archive = spawn_blocking(move || ciborium::from_reader(reader)).await??;
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

async fn restore_metadata_from_node(path: &Path, node: &Node) -> Result<()> {
    restore_metadata(path, node.metadata(), node.file_type()).await
}

async fn restore_metadata(path: &Path, metadata: &Metadata, file_type: FileType) -> Result<()> {
    let owner = Some(metadata.owner);
    let group = Some(metadata.group);
    let permissions = Permissions::from_mode(metadata.mode);

    if file_type.is_symlink() {
        lchown(path, owner, group)?;
    } else {
        chown(path, owner, group)?;
    }

    fs::set_permissions(path, permissions).await?;
    Ok(())
}

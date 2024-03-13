use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use log::debug;
use tokio::{fs, spawn, sync::Semaphore};

use crate::{
    error::{Error, Result},
    file::{restore_metadata, restore_metadata_from_node, try_exists, FileType, Metadata, Node},
    hash::{self, Hash},
};

use super::{
    blocks::{download_block_recursive, ActiveDownload},
    Args, State,
};

pub struct PendingDownload {
    pub metadata: Metadata,
    pub hash: Option<Hash>,
    pub path: PathBuf,
}

pub async fn restore_recursive(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    sender: Sender<PendingDownload>,
) -> Result<()> {
    for (path, node) in args.archive.walk() {
        let path = args.output_path.join(path);
        if let Some(pending_file) =
            restore_from_node(args.clone(), state.clone(), &path, node).await?
        {
            sender.send(pending_file).await?;
        }
    }

    Ok(())
}

async fn restore_from_node(
    _args: Arc<Args>,
    _state: Arc<Mutex<State>>,
    path: &Path,
    node: &Node,
) -> Result<Option<PendingDownload>> {
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
            return Ok(Some(pending_file));
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

    Ok(None)
}

pub async fn download_pending_files(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    receiver: Receiver<PendingDownload>,
) {
    let permit_count = args.max_concurrency;
    let semaphore = Arc::new(Semaphore::new(permit_count as usize));

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

    let _ = semaphore.acquire_many(permit_count).await.unwrap();
}

async fn download_pending_file(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    pending_file: PendingDownload,
) -> Result<()> {
    let mut file = ActiveDownload::new(&pending_file).await?;
    state.lock().unwrap().stats.files_created += 1;

    if let Some(hash) = pending_file.hash {
        download_block_recursive(args, state.clone(), &mut file, hash, None).await?;
        file.sync_all().await?;
    }

    restore_metadata(&pending_file.path, &pending_file.metadata, FileType::File).await?;
    let hash_str = hash::format(&pending_file.hash);
    debug!("{hash_str} -> {}", pending_file.path.display());
    Ok(())
}

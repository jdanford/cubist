use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_channel::{Receiver, Sender};
use log::debug;
use tokio::{
    fs, spawn,
    sync::{RwLock, Semaphore},
};

use crate::{
    error::{Error, Result},
    file::{restore_metadata, restore_metadata_from_node, try_exists, FileType, Metadata, Node},
    hash::{self, Hash},
    walker::FileWalker,
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
    state: Arc<RwLock<State>>,
    sender: Sender<PendingDownload>,
) -> Result<()> {
    let root_paths = if args.paths.is_empty() {
        Cow::Owned(vec![PathBuf::new()])
    } else {
        Cow::Borrowed(&args.paths)
    };

    for root_path in root_paths.iter() {
        for (path, node) in walk(args.as_ref(), root_path)? {
            let maybe_file = restore_from_node(args.clone(), state.clone(), &path, node).await?;
            if let Some(pending_file) = maybe_file {
                sender.send(pending_file).await?;
            }
        }
    }

    Ok(())
}

fn walk<'a>(
    args: &'a Args,
    path: &Path,
) -> Result<Box<dyn Iterator<Item = (PathBuf, &'a Node)> + 'a>> {
    let node = args
        .archive
        .get(path)
        .ok_or(Error::FileDoesNotExist(path.to_owned()))?;

    if let Node::Directory { children, .. } = node {
        let walker = FileWalker::new(children);
        Ok(Box::new(walker))
    } else {
        let singleton = vec![(path.to_owned(), node)].into_iter();
        Ok(Box::new(singleton))
    }
}

async fn restore_from_node(
    _args: Arc<Args>,
    _state: Arc<RwLock<State>>,
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
    state: Arc<RwLock<State>>,
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
    state: Arc<RwLock<State>>,
    pending_file: PendingDownload,
) -> Result<()> {
    let mut file = ActiveDownload::new(&pending_file).await?;
    state.write().await.stats.files_created += 1;

    if let Some(hash) = pending_file.hash {
        download_block_recursive(args, state.clone(), &mut file, hash, None).await?;
        file.sync_all().await?;
    }

    restore_metadata(&pending_file.path, &pending_file.metadata, FileType::File).await?;
    let hash_str = hash::format(&pending_file.hash);
    let local_path = pending_file.path.display();
    debug!("{hash_str} -> {local_path}");
    Ok(())
}

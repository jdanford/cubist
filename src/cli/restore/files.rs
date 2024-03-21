use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_channel::{Receiver, Sender};
use log::debug;
use tokio::{fs, sync::Semaphore, task::JoinSet};

use crate::{
    error::{Error, Result, OK},
    file::{
        restore_metadata, restore_metadata_from_node, try_exists, FileType, Metadata, Node,
        WalkNode,
    },
    hash::{self, Hash},
};

use super::{
    blocks::{download_block_recursive, ActiveDownload},
    Args, State,
};

#[derive(Debug)]
pub struct PendingDownload {
    pub metadata: Metadata,
    pub hash: Option<Hash>,
    pub path: PathBuf,
}

pub async fn restore_recursive(
    args: Arc<Args>,
    state: Arc<State>,
    sender: Sender<PendingDownload>,
) -> Result<()> {
    let root_paths = if args.paths.is_empty() {
        Cow::Owned(vec![PathBuf::new()])
    } else {
        Cow::Borrowed(&args.paths)
    };

    for root_path in root_paths.iter() {
        for (child_path, node) in walk(args.as_ref(), root_path)? {
            let path = root_path.join(child_path);
            let maybe_file = restore_from_node(args.clone(), state.clone(), &path, node).await?;
            if let Some(pending_file) = maybe_file {
                sender.send(pending_file).await?;
            }
        }
    }

    Ok(())
}

fn walk<'a>(args: &'a Args, path: &Path) -> Result<impl Iterator<Item = (PathBuf, &'a Node)> + 'a> {
    let node = args
        .archive
        .get(path)
        .ok_or(Error::FileDoesNotExist(path.to_owned()))?;
    Ok(WalkNode::new(node, args.order))
}

async fn restore_from_node(
    _args: Arc<Args>,
    _state: Arc<State>,
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
    state: Arc<State>,
    receiver: Receiver<PendingDownload>,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(args.tasks));
    let mut tasks = JoinSet::new();

    while let Ok(pending_file) = receiver.recv().await {
        let args = args.clone();
        let state = state.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            download_pending_file(args, state, pending_file).await?;
            drop(permit);
            OK
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

async fn download_pending_file(
    args: Arc<Args>,
    state: Arc<State>,
    pending_file: PendingDownload,
) -> Result<()> {
    let mut file = ActiveDownload::new(&pending_file).await?;
    state.stats.write().await.files_created += 1;

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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_channel::{Receiver, Sender};
use clap::builder::styling::AnsiColor;
use log::debug;
use tokio::{fs, sync::Semaphore, task::JoinSet};

use crate::{
    cli::format::{format_path, format_size},
    error::{Error, Result},
    file::{restore_metadata, restore_metadata_from_node, try_exists, FileType, Metadata, Node},
    hash::Hash,
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
        vec![None]
    } else {
        args.paths
            .iter()
            .map(|path| Option::Some(path.as_path()))
            .collect()
    };

    for root_path in root_paths {
        let walker = args.archive.walk(root_path, args.order)?;
        for (child_path, node) in walker {
            let path = if let Some(path) = root_path {
                path.join(&child_path)
            } else {
                child_path
            };

            let maybe_file = restore_from_node(args.clone(), state.clone(), &path, node).await?;
            if let Some(pending_file) = maybe_file {
                sender.send(pending_file).await?;
            }
        }
    }

    Ok(())
}

async fn restore_from_node(
    args: Arc<Args>,
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
            if !args.dry_run {
                fs::symlink(src, path).await?;
                restore_metadata_from_node(path, node).await?;
            }

            let formatted_path = format_path(path);
            let style = AnsiColor::Cyan.on_default();
            debug!("{style}created symlink{style:#} {formatted_path}");
        }
        Node::Directory { .. } => {
            if !args.dry_run {
                fs::create_dir(path).await?;
                restore_metadata_from_node(path, node).await?;
            }

            let formatted_path = format_path(path);
            let style = AnsiColor::Magenta.on_default();
            debug!("{style}created directory{style:#} {formatted_path}");
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
            Result::Ok(())
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
    let mut size = 0;

    if !args.dry_run {
        let mut file = ActiveDownload::new(&pending_file).await?;

        if let Some(hash) = pending_file.hash {
            size = download_block_recursive(args.clone(), state.clone(), &mut file, hash, None)
                .await?;
            file.sync_all().await?;
        }

        restore_metadata(&pending_file.path, &pending_file.metadata, FileType::File).await?;
    }

    state.stats.write().await.files_created += 1;

    let formatted_path = format_path(&pending_file.path);
    let formatted_size = format_size(size);
    let msg_style = AnsiColor::Blue.on_default();
    let size_style = AnsiColor::BrightBlack.on_default();
    debug!("{msg_style}downloaded file{msg_style:#} {formatted_path} {size_style}({formatted_size}){size_style:#}");
    Ok(())
}

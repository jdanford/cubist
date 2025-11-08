use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_channel::{Receiver, Sender};
use clap::builder::styling::AnsiColor;
use log::debug;
use tokio::fs;

use crate::{
    block::Block,
    error::{Error, Result, handle_error},
    file::{FileType, Metadata, Node, restore_metadata, restore_metadata_from_node, try_exists},
    format::{format_path, format_size},
    hash::Hash,
    task::BoundedJoinSet,
};

use super::{
    RestoreState,
    blocks::{ActiveDownload, download_block_recursive},
};

#[derive(Debug)]
pub struct PendingDownload {
    pub metadata: Metadata,
    pub hash: Option<Hash<Block>>,
    pub path: PathBuf,
}

pub async fn restore_all<P: AsRef<Path>>(
    state: Arc<RestoreState>,
    sender: Sender<PendingDownload>,
    paths: &[P],
) -> Result<()> {
    if paths.is_empty() {
        restore_recursive(state, sender, None).await?;
    } else {
        for root in paths {
            restore_recursive(state.clone(), sender.clone(), Some(root.as_ref())).await?;
        }
    }

    Ok(())
}

async fn restore_recursive(
    state: Arc<RestoreState>,
    sender: Sender<PendingDownload>,
    root: Option<&Path>,
) -> Result<()> {
    let walker = state.archive.walk(root, state.order)?;
    for (child_path, node) in walker {
        let path = if let Some(path) = root {
            path.join(&child_path)
        } else {
            child_path
        };

        let maybe_file = restore_from_node(state.clone(), &path, node).await?;
        if let Some(pending_file) = maybe_file {
            sender.send(pending_file).await?;
        }
    }

    Ok(())
}

async fn restore_from_node(
    state: Arc<RestoreState>,
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
            if !state.dry_run {
                fs::symlink(src, path).await?;
                restore_metadata_from_node(path, node).await?;
            }

            let formatted_path = format_path(path);
            let style = AnsiColor::Cyan.on_default();
            debug!("{style}created symlink{style:#} {formatted_path}");
        }
        Node::Directory { .. } => {
            if !state.dry_run {
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
    state: Arc<RestoreState>,
    receiver: Receiver<PendingDownload>,
) -> Result<()> {
    let mut tasks = BoundedJoinSet::new(state.task_count);

    while let Ok(pending_file) = receiver.recv().await {
        let state = state.clone();
        tasks
            .spawn(download_pending_file(state, pending_file))
            .await?;

        while let Some(result) = tasks.try_join_next() {
            handle_error(result?);
        }
    }

    while let Some(result) = tasks.join_next().await {
        handle_error(result?);
    }

    Ok(())
}

async fn download_pending_file(
    state: Arc<RestoreState>,
    pending_file: PendingDownload,
) -> Result<()> {
    let mut size = 0;

    if !state.dry_run {
        let mut file = ActiveDownload::new(&pending_file).await?;

        if let Some(ref hash) = pending_file.hash {
            size = download_block_recursive(state.clone(), &mut file, hash, None).await?;
            file.sync_all().await?;
        }

        restore_metadata(&pending_file.path, &pending_file.metadata, FileType::File).await?;
    }

    state.stats.write().await.files_created += 1;

    let formatted_path = format_path(&pending_file.path);
    let formatted_size = format_size(size);
    let msg_style = AnsiColor::Blue.on_default();
    let size_style = AnsiColor::BrightBlack.on_default();
    debug!(
        "{msg_style}downloaded file{msg_style:#} {formatted_path} {size_style}({formatted_size}){size_style:#}"
    );
    Ok(())
}

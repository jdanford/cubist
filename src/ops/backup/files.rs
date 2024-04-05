use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
};

use async_channel::{Receiver, Sender};
use async_walkdir::{DirEntry, WalkDir};
use clap::builder::styling::AnsiColor;
use log::{debug, warn};
use tokio::{
    fs::{self, File},
    io::BufReader,
    sync::Semaphore,
    task::JoinSet,
};
use tokio_stream::StreamExt;

use crate::{
    block::{self, Block},
    error::{Error, Result},
    file::{read_metadata, Node},
    format::{format_path, format_size},
    hash::Hash,
};

use super::{blocks::UploadTree, BackupState};

#[derive(Debug)]
pub struct PendingUpload {
    local_path: PathBuf,
    archive_path: PathBuf,
}

pub async fn backup_all<P: AsRef<Path>>(
    state: Arc<BackupState>,
    sender: Sender<PendingUpload>,
    paths: &[P],
) -> Result<()> {
    for path in paths {
        backup_recursive(state.clone(), sender.clone(), path.as_ref()).await?;
    }

    Ok(())
}

pub async fn backup_recursive(
    state: Arc<BackupState>,
    sender: Sender<PendingUpload>,
    path: &Path,
) -> Result<()> {
    let mut walker = WalkDir::new(path);
    loop {
        match walker.try_next().await {
            Ok(Some(entry)) => {
                let maybe_file = backup_from_entry(state.clone(), entry, path).await?;
                if let Some(pending_file) = maybe_file {
                    sender.send(pending_file).await?;
                }
            }
            Ok(None) => break,
            Err(err) => {
                handle_walkdir_error(err)?;
            }
        }
    }

    Ok(())
}

async fn backup_from_entry(
    state: Arc<BackupState>,
    entry: DirEntry,
    base_path: &Path,
) -> Result<Option<PendingUpload>> {
    let local_path = entry.path();
    let archive_path = local_path.strip_prefix(base_path)?.to_owned();
    let formatted_path = format_path(&local_path);

    let file_type = entry.file_type().await?;
    if file_type.is_file() {
        let pending_file = PendingUpload {
            local_path,
            archive_path,
        };
        return Ok(Some(pending_file));
    } else if file_type.is_symlink() {
        let metadata = read_metadata(&local_path).await?;
        let path = fs::read_link(&local_path).await?;
        let node = Node::Symlink { metadata, path };
        let archive = &mut state.archive.write().await;
        archive.insert(archive_path, node)?;

        let style = AnsiColor::Cyan.on_default();
        debug!("{style}added symlink{style:#} {formatted_path}");
    } else if file_type.is_dir() {
        let metadata = read_metadata(&local_path).await?;
        let children = BTreeMap::new();
        let node = Node::Directory { metadata, children };
        let archive = &mut state.archive.write().await;
        archive.insert(archive_path, node)?;

        let style = AnsiColor::Magenta.on_default();
        debug!("{style}added directory{style:#} {formatted_path}");
    } else {
        warn!("skipped special file {formatted_path}");
    };

    Ok(None)
}

pub async fn upload_pending_files(
    state: Arc<BackupState>,
    receiver: Receiver<PendingUpload>,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(state.task_count));
    let mut tasks = JoinSet::new();

    while let Ok(pending_file) = receiver.recv().await {
        let state = state.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            let result = upload_pending_file(state, pending_file).await;
            if let Err(Error::WalkDir(err)) = result {
                handle_walkdir_error(err)?;
            }

            drop(permit);
            Result::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

async fn upload_pending_file(state: Arc<BackupState>, pending_file: PendingUpload) -> Result<()> {
    let PendingUpload {
        local_path,
        archive_path,
    } = pending_file;

    let metadata = read_metadata(&local_path).await?;
    let mut file = File::open(&local_path).await?;
    let (hash, size) = upload_file(state.clone(), &mut file).await?;
    let node = Node::File { metadata, hash };
    let archive = &mut state.archive.write().await;
    archive.insert(archive_path, node)?;

    let formatted_path = format_path(&local_path);
    let formatted_size = format_size(size);
    let msg_style = AnsiColor::Blue.on_default();
    let size_style = AnsiColor::BrightBlack.on_default();
    debug!("{msg_style}uploaded file{msg_style:#} {formatted_path} {size_style}({formatted_size}){size_style:#}");
    Ok(())
}

pub async fn upload_file(
    state: Arc<BackupState>,
    file: &mut File,
) -> Result<(Option<Hash<Block>>, u64)> {
    let reader = BufReader::new(file);
    let mut chunker = block::chunker(reader, state.target_block_size);
    let mut chunks = pin!(chunker.as_stream());
    let mut tree = UploadTree::new(state.clone());
    let mut size = 0;

    while let Some(chunk) = chunks.try_next().await? {
        size += chunk.data.len() as u64;
        tree.add_leaf(chunk.data).await?;
    }

    state.stats.write().await.bytes_read += size;
    state.stats.write().await.files_read += 1;

    let hash = tree.finalize().await?;
    Ok((hash, size))
}

fn handle_walkdir_error(err: async_walkdir::Error) -> Result<()> {
    if let Some(io_err) = err.io() {
        if let Some(path) = err.path() {
            let formatted_path = format_path(path);
            warn!("skipped file {formatted_path} ({io_err})");
        } else {
            warn!("skipped file ({io_err})");
        }

        Ok(())
    } else {
        Err(err.into())
    }
}

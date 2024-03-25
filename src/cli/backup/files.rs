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
    block,
    cli::format::{format_path, format_size},
    error::{Result, OK},
    file::{read_metadata, Node},
    hash::Hash,
};

use super::{blocks::UploadTree, Args, State};

#[derive(Debug)]
pub struct PendingUpload {
    local_path: PathBuf,
    archive_path: PathBuf,
}

pub async fn backup_recursive(
    args: Arc<Args>,
    state: Arc<State>,
    sender: Sender<PendingUpload>,
    path: &Path,
) -> Result<()> {
    let mut walker = WalkDir::new(path);
    while let Some(entry) = walker.try_next().await? {
        let maybe_file = backup_from_entry(args.clone(), state.clone(), entry, path).await?;
        if let Some(pending_file) = maybe_file {
            sender.send(pending_file).await?;
        }
    }

    Ok(())
}

async fn backup_from_entry(
    _args: Arc<Args>,
    state: Arc<State>,
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
        let archive = &mut state.archive_builder.write().await;
        archive.insert(archive_path, node)?;

        let style = AnsiColor::Cyan.on_default();
        debug!("{style}added symlink{style:#} {formatted_path}");
    } else if file_type.is_dir() {
        let metadata = read_metadata(&local_path).await?;
        let children = BTreeMap::new();
        let node = Node::Directory { metadata, children };
        let archive = &mut state.archive_builder.write().await;
        archive.insert(archive_path, node)?;

        let style = AnsiColor::Magenta.on_default();
        debug!("{style}added directory{style:#} {formatted_path}");
    } else {
        warn!("skipped special file {formatted_path}");
    };

    Ok(None)
}

pub async fn upload_pending_files(
    args: Arc<Args>,
    state: Arc<State>,
    receiver: Receiver<PendingUpload>,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(args.tasks));
    let mut tasks = JoinSet::new();

    while let Ok(pending_file) = receiver.recv().await {
        let args = args.clone();
        let state = state.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            upload_pending_file(args, state, pending_file).await?;
            drop(permit);
            OK
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

async fn upload_pending_file(
    args: Arc<Args>,
    state: Arc<State>,
    pending_file: PendingUpload,
) -> Result<()> {
    let PendingUpload {
        local_path,
        archive_path,
    } = pending_file;

    let metadata = read_metadata(&local_path).await?;
    let mut file = File::open(&local_path).await?;
    let (hash, size) = upload_file(args.clone(), state.clone(), &mut file).await?;
    let node = Node::File { metadata, hash };
    let archive = &mut state.archive_builder.write().await;
    archive.insert(archive_path, node)?;

    let formatted_path = format_path(&local_path);
    let formatted_size = format_size(size);
    let msg_style = AnsiColor::Blue.on_default();
    let size_style = AnsiColor::BrightBlack.on_default();
    debug!("{msg_style}uploaded file{msg_style:#} {formatted_path} {size_style}({formatted_size}){size_style:#}");
    Ok(())
}

pub async fn upload_file(
    args: Arc<Args>,
    state: Arc<State>,
    file: &mut File,
) -> Result<(Option<Hash>, u64)> {
    let reader = BufReader::new(file);
    let mut chunker = block::chunker(reader, args.target_block_size);
    let mut chunks = pin!(chunker.as_stream());
    let mut tree = UploadTree::new(args.clone(), state.clone());
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

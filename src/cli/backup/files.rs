use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
};

use async_channel::{Receiver, Sender};
use async_walkdir::{DirEntry, WalkDir};
use log::{debug, warn};
use tokio::{
    fs::{self, File},
    io::BufReader,
    spawn,
    sync::Semaphore,
};
use tokio_stream::StreamExt;

use crate::{
    block,
    error::Result,
    file::{read_metadata, Node},
    hash::{self, Hash},
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
    } else if file_type.is_dir() {
        let metadata = read_metadata(&local_path).await?;
        let children = BTreeMap::new();
        let node = Node::Directory { metadata, children };
        let archive = &mut state.archive.write().await;
        archive.insert(archive_path, node)?;
    } else {
        warn!("skipping special file `{}`", local_path.display());
    };

    Ok(None)
}

pub async fn upload_pending_files(
    args: Arc<Args>,
    state: Arc<State>,
    receiver: Receiver<PendingUpload>,
) -> Result<()> {
    let permit_count = args.jobs;
    let semaphore = Arc::new(Semaphore::new(permit_count as usize));

    while let Ok(pending_file) = receiver.recv().await {
        let args = args.clone();
        let state = state.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        spawn(async move {
            upload_pending_file(args, state, pending_file)
                .await
                .unwrap();
            drop(permit);
        });
    }

    let _ = semaphore.acquire_many(permit_count).await?;
    Ok(())
}

async fn upload_pending_file(
    args: Arc<Args>,
    state: Arc<State>,
    pending_file: PendingUpload,
) -> Result<()> {
    let metadata = read_metadata(&pending_file.local_path).await?;
    let mut file = File::open(&pending_file.local_path).await?;
    let hash = upload_file(args.clone(), state.clone(), &mut file).await?;
    let node = Node::File { metadata, hash };
    let archive = &mut state.archive.write().await;
    archive.insert(pending_file.archive_path, node)?;

    let hash_str = hash::format(&hash);
    let local_path = pending_file.local_path.display();
    debug!("{hash_str} <- {local_path}");
    Ok(())
}

pub async fn upload_file(
    args: Arc<Args>,
    state: Arc<State>,
    file: &mut File,
) -> Result<Option<Hash>> {
    let reader = BufReader::new(file);
    let mut chunker = block::chunker(reader, args.target_block_size);
    let mut chunks = pin!(chunker.as_stream());
    let mut tree = UploadTree::new(args.clone(), state.clone());

    while let Some(chunk) = chunks.try_next().await? {
        state.stats.write().await.bytes_read += chunk.data.len() as u64;
        tree.add_leaf(chunk.data).await?;
    }

    state.stats.write().await.files_read += 1;
    let hash = tree.finalize().await?;
    Ok(hash)
}

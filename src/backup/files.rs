use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    pin::pin,
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use log::{info, warn};
use tokio::{
    fs::{self, File},
    io::BufReader,
    spawn,
    sync::Semaphore,
};
use tokio_stream::StreamExt;
use walkdir::{DirEntry, WalkDir};

use crate::{
    block,
    error::Result,
    file::{read_metadata, Node},
    hash::{self, Hash},
};

use super::{blocks::UploadTree, Args, State};

pub struct PendingUpload {
    local_path: PathBuf,
    archive_path: PathBuf,
}

pub async fn backup_recursive(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    sender: Sender<PendingUpload>,
    path: &Path,
) -> Result<()> {
    let walker = WalkDir::new(path);
    for entry_result in walker {
        let entry = entry_result?;
        if entry.file_type().is_dir() && entry.depth() == 0 {
            continue;
        }

        if let Some(pending_file) =
            backup_from_entry(args.clone(), state.clone(), entry, path).await?
        {
            sender.send(pending_file).await?;
        }
    }

    Ok(())
}

async fn backup_from_entry(
    _args: Arc<Args>,
    state: Arc<Mutex<State>>,
    entry: DirEntry,
    base_path: &Path,
) -> Result<Option<PendingUpload>> {
    let local_path = entry.path();
    let archive_path = local_path.strip_prefix(base_path)?;
    let file_type = entry.file_type();
    if file_type.is_file() {
        let pending_file = PendingUpload {
            local_path: local_path.to_owned(),
            archive_path: archive_path.to_owned(),
        };
        return Ok(Some(pending_file));
    } else if file_type.is_symlink() {
        let metadata = read_metadata(local_path).await?;
        let path = fs::read_link(local_path).await?;
        let node = Node::Symlink { metadata, path };
        let archive = &mut state.lock().unwrap().archive;
        archive.insert(archive_path.to_owned(), node)?;
    } else if file_type.is_dir() {
        let metadata = read_metadata(local_path).await?;
        let children = BTreeMap::new();
        let node = Node::Directory { metadata, children };
        let archive = &mut state.lock().unwrap().archive;
        archive.insert(archive_path.to_owned(), node)?;
    } else {
        warn!("skipping special file `{}`", local_path.display());
    };

    Ok(None)
}

pub async fn upload_pending_files(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    receiver: Receiver<PendingUpload>,
) {
    let semaphore = Arc::new(Semaphore::new(args.max_concurrency));
    while let Ok(pending_file) = receiver.recv().await {
        let args = args.clone();
        let state = state.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        spawn(async move {
            upload_pending_file(args, state, pending_file)
                .await
                .unwrap();
            drop(permit);
        });
    }

    let permit_count = u32::try_from(args.max_concurrency).unwrap();
    let _ = semaphore.acquire_many(permit_count).await.unwrap();
}

async fn upload_pending_file(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    pending_file: PendingUpload,
) -> Result<()> {
    let metadata = read_metadata(&pending_file.local_path).await?;
    let mut file = File::open(&pending_file.local_path).await?;
    let hash = upload_file(args.clone(), state.clone(), &mut file).await?;
    let node = Node::File { metadata, hash };

    state
        .lock()
        .unwrap()
        .archive
        .insert(pending_file.archive_path, node)?;

    let hash_str = hash::format(&hash);
    info!("{hash_str} <- {}", pending_file.local_path.display());
    Ok(())
}

pub async fn upload_file(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    file: &mut File,
) -> Result<Option<Hash>> {
    let reader = BufReader::new(file);
    let mut chunker = block::chunker(reader, args.target_block_size);
    let mut chunks = pin!(chunker.as_stream());
    let mut tree = UploadTree::new(args.clone(), state.clone());

    while let Some(chunk_result) = chunks.next().await {
        let chunk = chunk_result?;
        state.lock().unwrap().stats.bytes_read += chunk.data.len() as u64;
        tree.add_leaf(chunk.data).await?;
    }

    let hash = tree.finalize().await?;
    Ok(hash)
}

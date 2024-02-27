use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    pin::pin,
};

use chrono::{DateTime, Utc};
use tokio::{
    fs::{self, File},
    task::spawn_blocking,
};
use tokio_stream::StreamExt;
use walkdir::{DirEntry, WalkDir};

use crate::{
    block::{self, BlockHash},
    error::{Error, Result},
    file::{read_metadata, Archive, FileHash, Node},
    hash,
    storage::Storage,
};

struct BackupArgs<S> {
    storage: S,
    compression_level: u32,
    target_block_size: u32,
    bucket: String,
    paths: Vec<PathBuf>,
}

struct BackupState {
    archive: Archive,
}

pub async fn backup<S: Storage>(
    storage: S,
    compression_level: u32,
    target_block_size: u32,
    bucket: String,
    paths: Vec<PathBuf>,
) -> Result<()> {
    let time = Utc::now();
    let archive = Archive::new();

    let args = BackupArgs {
        storage,
        compression_level,
        target_block_size,
        bucket,
        paths,
    };

    let mut state = BackupState { archive };

    for path in &args.paths {
        upload_recursive(&args, &mut state, path).await?;
    }

    upload_archive(&args, state.archive, time).await?;
    Ok(())
}

async fn upload_archive<S: Storage>(
    args: &BackupArgs<S>,
    archive: Archive,
    time: DateTime<Utc>,
) -> Result<()> {
    let data = spawn_blocking(move || {
        let mut data = vec![];
        ciborium::into_writer(&archive, &mut data)?;
        Result::Ok(data)
    })
    .await??;

    let timestamp = time.format("%Y-%m-%dT%H:%M:%S").to_string();
    let key = format!("archive:{}", timestamp);
    args.storage.put(&args.bucket, &key, data).await?;

    let latest_key = "archive:latest";
    args.storage
        .put(&args.bucket, latest_key, timestamp.into())
        .await?;
    Ok(())
}

async fn upload_recursive<S: Storage>(
    args: &BackupArgs<S>,
    state: &mut BackupState,
    path: &Path,
) -> Result<()> {
    let walker = WalkDir::new(path);

    for entry_result in walker {
        let entry = entry_result?;
        if entry.file_type().is_dir() && entry.depth() == 0 {
            continue;
        }

        upload_from_dir_entry(args, state, entry, path).await?;
    }

    Ok(())
}

async fn upload_from_dir_entry<S: Storage>(
    args: &BackupArgs<S>,
    state: &mut BackupState,
    entry: DirEntry,
    base_path: &Path,
) -> Result<()> {
    let local_path = entry.path();
    let archive_path = local_path.strip_prefix(base_path)?;
    println!("{}", archive_path.to_string_lossy());

    let file_type = entry.file_type();
    if file_type.is_file() {
        upload_from_path(args, state, local_path, archive_path).await?;
    } else if file_type.is_symlink() {
        let metadata = read_metadata(local_path).await?;
        let path = fs::read_link(local_path).await?;
        let node = Node::Symlink { metadata, path };
        state.archive.insert(archive_path, node)?;
    } else if file_type.is_dir() {
        let metadata = read_metadata(local_path).await?;
        let children = BTreeMap::new();
        let node = Node::Directory { metadata, children };
        state.archive.insert(archive_path, node)?;
    } else {
        // TODO: skip?
        return Err(Error::WeirdFile(local_path.to_owned()));
    };

    Ok(())
}

async fn upload_from_path<S: Storage>(
    args: &BackupArgs<S>,
    state: &mut BackupState,
    local_path: &Path,
    archive_path: &Path,
) -> Result<()> {
    let metadata = read_metadata(local_path).await?;
    let mut file = File::open(local_path).await?;
    let (file_hash, block_hashes) = upload_blocks(args, &mut file).await?;
    upload_file(args, file_hash, block_hashes).await?;

    let node = Node::File {
        metadata,
        hash: file_hash,
    };

    state.archive.insert(archive_path, node)?;
    Ok(())
}

async fn upload_file<S: Storage>(
    args: &BackupArgs<S>,
    file_hash: FileHash,
    block_hashes: Vec<BlockHash>,
) -> Result<()> {
    let key = file_hash.key();
    if !args.storage.exists(&args.bucket, &key).await? {
        let chunk_size = args.target_block_size as usize;
        let chunk_hash_count = chunk_size / hash::SIZE;
        let chunks = block_hashes.chunks(chunk_hash_count).map(concat_hashes);
        args.storage
            .put_streaming(&args.bucket, &key, chunks)
            .await?;
    }

    Ok(())
}

async fn upload_blocks<S: Storage>(
    args: &BackupArgs<S>,
    file: &mut File,
) -> Result<(FileHash, Vec<BlockHash>)> {
    let mut chunker = block::chunker(file, args.target_block_size);
    let mut chunks = pin!(chunker.as_stream());
    let mut hasher = blake3::Hasher::new();
    let mut block_hashes = Vec::new();

    while let Some(chunk_result) = chunks.next().await {
        let chunk = chunk_result?;
        let block_hash = upload_block(args, &chunk.data).await?;
        hasher.update(block_hash.as_bytes());
        block_hashes.push(block_hash);
    }

    let file_hash = hasher.finalize().into();
    Ok((file_hash, block_hashes))
}

async fn upload_block<S: Storage>(args: &BackupArgs<S>, data: &[u8]) -> Result<BlockHash> {
    let block_hash = block::hash(data).await?;
    let key = block_hash.key();
    if !args.storage.exists(&args.bucket, &key).await? {
        let compressed_data = block::compress(data, args.compression_level).await?;
        args.storage
            .put(&args.bucket, &key, compressed_data)
            .await?;
    }

    Ok(block_hash)
}

fn concat_hashes(hashes: &[BlockHash]) -> Vec<u8> {
    concat_arrays(hashes.iter().map(|hash| hash.as_bytes()).cloned())
}

fn concat_arrays<T: Clone, const N: usize, I>(arrays: I) -> Vec<T>
where
    I: ExactSizeIterator<Item = [T; N]>,
{
    let len = arrays.len() * N;
    let mut output = Vec::with_capacity(len);

    for array in arrays {
        output.extend_from_slice(&array);
    }

    output
}

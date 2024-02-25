use std::{
    collections::BTreeMap, path::{Path, PathBuf}, pin::pin
};

use chrono::{DateTime, Utc};
use tokio::fs::{self, File};
use tokio_stream::StreamExt;
use walkdir::{DirEntry, WalkDir};

use crate::{
    block::{self, BlockHash},
    cloud::Cloud,
    error::Error,
    file::{Archive, FileHash, Metadata, Node},
    hash,
};

struct BackupArgs {
    cloud: Cloud,
    compression_level: u32,
    target_block_size: u32,
    bucket: String,
    paths: Vec<PathBuf>,
}

struct BackupState {
    archive: Archive,
}

pub async fn backup(
    cloud: Cloud,
    compression_level: u32,
    target_block_size: u32,
    bucket: String,
    paths: Vec<PathBuf>,
) -> Result<(), Error> {
    let archive = Archive::new();
    let time = Utc::now();

    let args = BackupArgs {
        cloud,
        compression_level,
        target_block_size,
        bucket,
        paths,
    };

    let mut state = BackupState { archive };

    for path in args.paths.iter() {
        upload_recursive(&args, &mut state, path).await?;
    }

    upload_archive(&args, &state, time).await?;
    Ok(())
}

async fn upload_archive(
    args: &BackupArgs,
    state: &BackupState,
    time: DateTime<Utc>,
) -> Result<(), Error> {
    let mut data = vec![];
    ciborium::into_writer(&state.archive, &mut data)?;

    let timestamp = time.format("%+").to_string();
    let key = format!("archive/{}", timestamp);
    args.cloud.put(&args.bucket, &key, data).await?;

    let latest_key = "archive/latest";
    args.cloud.put(&args.bucket, latest_key, timestamp.into()).await?;
    Ok(())
}

async fn upload_recursive(
    args: &BackupArgs,
    state: &mut BackupState,
    path: &Path,
) -> Result<(), Error> {
    let walker = WalkDir::new(path);
    for entry_result in walker {
        let entry = entry_result?;
        upload_from_dir_entry(args, state, entry).await?;
    }

    Ok(())
}

async fn upload_from_dir_entry(
    args: &BackupArgs,
    state: &mut BackupState,
    entry: DirEntry,
) -> Result<(), Error> {
    let path = entry.path();
    let file_type = entry.file_type();
    let native_metadata = fs::metadata(path).await?;
    let metadata = Metadata::from_native(native_metadata);

    let node = (if file_type.is_file() {
        let mut file = File::open(path).await?;
        let (file_hash, block_hashes) = upload_blocks(args, &mut file).await?;
        upload_file(args, file_hash, block_hashes).await?;

        Ok(Node::File {
            metadata,
            hash: file_hash,
        })
    } else if file_type.is_symlink() {
        let path = fs::read_link(path).await?;
        Ok(Node::Symlink { metadata, path })
    } else if file_type.is_dir() {
        Ok(Node::Directory {
            metadata,
            children: BTreeMap::new(),
        })
    } else {
        Err(Error::weird_file(path))
    })?;

    state.archive.insert(path, node)?;
    Ok(())
}

async fn upload_file(
    args: &BackupArgs,
    file_hash: FileHash,
    block_hashes: Vec<BlockHash>,
) -> Result<(), Error> {
    let key = file_hash.key();
    if !args.cloud.exists(&args.bucket, &key).await? {
        let chunk_size = args.target_block_size as usize;
        let chunk_hash_count = chunk_size / hash::SIZE;
        let chunks = block_hashes.chunks(chunk_hash_count).map(concat_hashes);
        args.cloud.put_streaming(&args.bucket, &key, chunks).await?;
    }

    Ok(())
}

async fn upload_blocks(
    args: &BackupArgs,
    file: &mut File,
) -> Result<(FileHash, Vec<BlockHash>), Error> {
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

async fn upload_block(args: &BackupArgs, data: &[u8]) -> Result<BlockHash, Error> {
    let block_hash = block::hash(data).await?;
    let key = block_hash.key();
    if !args.cloud.exists(&args.bucket, &key).await? {
        let compressed_data = block::compress(data, args.compression_level).await?;
        args.cloud.put(&args.bucket, &key, compressed_data).await?;
    }

    Ok(block_hash)
}

fn concat_hashes(hashes: &[BlockHash]) -> Vec<u8> {
    concat_arrays(hashes.iter().map(|hash| hash.as_bytes()).cloned())
}

fn concat_arrays<T: Clone, const N: usize, I: ExactSizeIterator<Item = [T; N]>>(
    arrays: I,
) -> Vec<T> {
    let len = arrays.len() * N;
    let mut output = Vec::with_capacity(len);

    for array in arrays {
        output.extend_from_slice(&array);
    }

    output
}

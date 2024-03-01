use std::{path::PathBuf, sync::Arc, time::Duration};

use cubist::{backup::BackupArgs, backup::UploadTree, error::Result, storage::LocalStorage};
use rand::RngCore;

#[tokio::main]
async fn main() -> Result<()> {
    let storage = Box::new(LocalStorage::new(
        PathBuf::from("data"),
        Duration::from_millis(0),
    ));
    let args = Arc::new(BackupArgs {
        storage,
        compression_level: 3,
        target_block_size: 1048576,
        max_concurrency: 16,
        bucket: "cubist".into(),
        paths: vec![],
    });

    let mut tree = UploadTree::new(args, 4);
    let mut data = [0; 64];
    for _ in 0..16 {
        rand::thread_rng().fill_bytes(&mut data);
        tree.add(&data).await?;
    }

    tree.finalize().await?;

    Ok(())
}

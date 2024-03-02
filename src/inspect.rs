use blake3::Hash;

use crate::{block, error::Result, hash, storage::BoxedStorage};

pub async fn inspect(storage: BoxedStorage, hash: Hash) -> Result<()> {
    let key = block::key(&hash);
    let block = storage.get(&key).await?;
    let (&level, data) = block.split_first().unwrap();
    let hashes = if level == 0 {
        vec![]
    } else {
        hash::split(data).collect()
    };

    println!("level: {level}");

    for hash in hashes {
        println!("{hash}");
    }

    Ok(())
}

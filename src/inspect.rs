use blake3::Hash;

use crate::{error::Result, hash, storage::BoxedStorage};

pub async fn inspect(storage: BoxedStorage, bucket: String, hash: Hash) -> Result<()> {
    let key = hash.to_string();
    let block = storage.get(&bucket, &key).await?;
    let (&level, data) = block.split_first().unwrap();
    let hashes = if level == 0 {
        vec![]
    } else {
        data.chunks_exact(hash::SIZE)
            .map(|bytes| Hash::from_bytes(bytes.try_into().unwrap()))
            .collect()
    };

    println!("level: {level}");

    for hash in hashes {
        println!("{hash}");
    }

    Ok(())
}

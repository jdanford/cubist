use crate::{
    block,
    cli::{self, create_storage, init_logger},
    error::Result,
    hash,
};

pub async fn main(args: cli::InspectArgs) -> Result<()> {
    init_logger(args.logger);

    let storage = create_storage(args.storage).await;
    let key = block::key(&args.hash);
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

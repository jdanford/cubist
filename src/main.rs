use clap::Parser;
use cubist::{
    backup::backup,
    cli::{create_storage, Cli, Command},
    error::Result,
    inspect::inspect,
    logger,
    restore::restore,
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::new()
        .format(logger::format)
        .filter_level(log::LevelFilter::Info)
        .try_init()
        .unwrap();

    let cli = Cli::parse();
    match cli.command {
        Command::Backup {
            storage_args,
            compression_level,
            target_block_size,
            max_concurrency,
            paths,
        } => {
            let storage = create_storage(storage_args).await;
            backup(
                storage,
                compression_level,
                target_block_size,
                max_concurrency,
                paths,
            )
            .await
        }
        Command::Restore {
            storage_args,
            max_concurrency,
            path,
        } => {
            let storage = create_storage(storage_args).await;
            restore(storage, max_concurrency, path).await
        }
        Command::InspectBlock { storage_args, hash } => {
            let storage = create_storage(storage_args).await;
            inspect(storage, hash).await
        }
    }
}

use clap::Parser;
use cubist::{
    backup::backup,
    cli::{Cli, Command},
    cloud::Cloud,
    error::Error,
    restore::restore,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    let cloud = Cloud::from_env().await;
    match cli.command {
        Command::Backup {
            compression_level,
            target_block_size,
            bucket,
            paths,
        } => backup(cloud, compression_level, target_block_size, bucket, paths).await,
        Command::Restore { bucket, path } => restore(cloud, bucket, path).await,
    }
}

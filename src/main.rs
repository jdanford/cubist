use clap::Parser;
use cubist::{
    backup,
    cli::{Cli, Command},
    delete,
    error::Result,
    restore,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Backup(args) => backup::main(args).await,
        Command::Restore(args) => restore::main(args).await,
        Command::Delete(args) => delete::main(args).await,
    }
}

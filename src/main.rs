use std::process::ExitCode;

use cubist::cli;

#[tokio::main]
async fn main() -> ExitCode {
    cli::main().await
}

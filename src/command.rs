use crate::Server;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Start {
        #[clap(short, long, value_name = "FILE")]
        config: Option<PathBuf>,
    },
    Stop,
    Restart,
    Reload,
}

pub fn exec() {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Start { config: None }) {
        Command::Start { config } => {
            if let Err(e) = Server::start(config) {
                eprintln!("{}", e);
            }
        }
        Command::Stop => Server::stop(),
        Command::Restart => Server::restart(),
        Command::Reload => Server::reload(),
    }
}

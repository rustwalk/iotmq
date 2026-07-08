use crate::Server;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

pub const SOCK: &str = "./run/iotmq.sock";
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

impl Cli {
    pub fn run() {
        let cli = Cli::parse();
        match cli.command.unwrap_or(Command::Start { config: None }) {
            Command::Start { config } => {
                if let Err(e) = Server::run(config) {
                    eprintln!("Failed to run server: {}", e);
                }
            }
            Command::Stop => Cmd::send("stop"),
            Command::Restart => Cmd::send("restart"),
            Command::Reload => Cmd::send("reload"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Cmd {
    pub cmd: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Ret {
    pub ok: bool,
    pub msg: String,
}

impl Cmd {
    fn send(cmd: &str) {
        let mut stream = match UnixStream::connect(SOCK) {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!("Server is not running: {}", e);
                return;
            }
        };

        let send = Cmd { cmd: cmd.into() };

        if let Err(e) = serde_json::to_writer(&mut stream, &send) {
            eprintln!("Failed to send {} command: {}", cmd, e);
            return;
        };

        if let Err(e) = stream.write_all(b"\n") {
            eprintln!("Failed to send {} command: {}", cmd, e);
        }
    }
}

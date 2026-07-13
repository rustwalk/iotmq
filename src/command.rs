use crate::server::Server;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
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
                    eprintln!("Run server error: {}", e);
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
        let mut send = match serde_json::to_vec(&send) {
            Ok(send) => send,
            Err(e) => {
                eprintln!("Encode command {} error: {}", cmd, e);
                return;
            }
        };
        send.push(b'\n');

        if let Err(e) = stream.write_all(&send) {
            eprintln!("Send command {} error: {}", cmd, e);
        }

        let mut reader = BufReader::new(stream);
        let mut line = String::new();

        if let Err(e) = reader.read_line(&mut line) {
            eprintln!("Receive {} response  error: {}", cmd, e);
            return;
        }
        let line = line.trim_end();

        let ret: Ret = match serde_json::from_str(line) {
            Ok(ret) => ret,
            Err(e) => {
                eprintln!("Receive {} response error: {}", cmd, e);
                return;
            }
        };

        if !ret.ok {
            eprintln!("{}", ret.msg);
        }
    }
}

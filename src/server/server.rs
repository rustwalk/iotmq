use crate::command::Ret;
use crate::context::Event;
use crate::{Broker, ConfigManager, Context, WebServer, command::*, logger::Log};
use anyhow::Result;
use std::fs;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info};

pub struct Server;

impl Server {
    /// Server run
    pub fn run(config: Option<PathBuf>) -> Result<()> {
        let config_path = ConfigManager::static_config(config)?; // Get config path
        let config = ConfigManager::init(&config_path)?; // init config
        Log::init(&config.read().log)?; // init log

        info!("Server starting...");

        let ctx = Context::init(config); // init context
        let mut rx = ctx.subscribe();
        let rt = tokio::runtime::Runtime::new()?;
        let event = rt.block_on(async {
            // Broker server
            let broker_ctx = ctx.clone();
            let broker_task = tokio::spawn(async move {
                if let Err(e) = Broker::run(broker_ctx.clone()).await {
                    error!("MQTT Broker error: {:#}", e);
                    broker_ctx.stop();
                }
            });

            // Web server
            let web_ctx = ctx.clone();
            let web_task = tokio::spawn(async move {
                if let Err(e) = WebServer::run(web_ctx.clone()).await {
                    error!("Web server error: {}", e);
                    web_ctx.stop();
                }
            });

            // Command server
            let command_ctx = ctx.clone();
            let command_task = tokio::spawn(async move {
                if let Err(e) = Self::command(command_ctx.clone()).await {
                    error!("Command server error: {}", e);
                    command_ctx.stop();
                }
            });

            // Signal server
            let signal_ctx = ctx.clone();
            let signal_task = tokio::spawn(async move {
                if let Err(e) = Self::signal(signal_ctx.clone()).await {
                    error!("Signal server error: {}", e);
                    signal_ctx.stop();
                }
            });

            // Wait event
            let event = Context::shutdown(&mut rx).await;

            // Wait all task shutdown
            let _ = tokio::join!(web_task, broker_task, command_task, signal_task);

            event
        });

        info!("Server stopped");
        if event == Event::Restart {
            Self::restart()?;
        }
        Ok(())
    }

    pub fn restart() -> Result<()> {
        let exe = std::env::current_exe()?;
        let err = std::process::Command::new(exe).args(std::env::args().skip(1)).exec();
        Err(err.into())
    }

    /// Listen command
    async fn command(ctx: Context) -> Result<()> {
        if fs::exists(SOCK)? {
            fs::remove_file(SOCK)?;
        }

        if let Some(dir) = Path::new(SOCK).parent() {
            fs::create_dir_all(dir)?;
        }

        let listener = UnixListener::bind(SOCK)?;
        let mut rx = ctx.subscribe();
        loop {
            tokio::select! {
                stream = listener.accept() => {
                    let (stream, _) = stream?;
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::command_recv(ctx, stream).await {
                            error!("Command execute error: {}", e);
                        }
                    });
                }

                _ = Context::shutdown(&mut rx) => break,
            }
        }
        Ok(())
    }

    /// Command execute
    async fn command_recv(ctx: Context, stream: UnixStream) -> Result<()> {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();

        reader.read_line(&mut line).await?;
        let line = line.trim_end();
        info!("Server received command: {}", line);

        let cmd: Cmd = serde_json::from_str(&line)?;
        let mut ret = Ret { ok: true, msg: "".into() };

        match cmd.cmd.as_str() {
            "reload" => match ctx.config.reload() {
                Ok(_) => ctx.reload(),
                Err(e) => {
                    let err = format!("Reload config error: {}", e);
                    error!("{}", err);
                    ret.ok = false;
                    ret.msg = err;
                }
            },
            "stop" => {
                ctx.stop();
            }
            "restart" => {
                ctx.restart();
            }
            _ => {}
        }

        let mut ret = serde_json::to_vec(&ret)?;
        ret.push(b'\n');
        let mut stream = reader.into_inner();
        stream.write_all(&ret).await?;
        Ok(())
    }

    /// Signal listen
    async fn signal(ctx: Context) -> Result<()> {
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sighup = signal(SignalKind::hangup())?;
        let mut rx = ctx.subscribe();

        loop {
            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received signal: SIGTERM");
                    ctx.stop();
                    break;
                }

                _ = sigint.recv() => {
                    info!("Received signal: SIGINT");
                    ctx.stop();
                    break;
                }

                _ = sighup.recv() => {
                    info!("Server received signal: SIGHUP");
                    match ctx.config.reload() {
                        Ok(_) => ctx.reload(),
                        Err(e) => error!("Reload config error: {}", e)
                    }
                }

                _ = Context::shutdown(&mut rx) => break,
            }
        }
        Ok(())
    }
}

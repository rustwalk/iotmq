use crate::config::{Config, ConfigManager};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub enum Command {
    Stop,
    Reload,
    Restart,
}

#[derive(Clone)]
pub struct Context {
    tx: broadcast::Sender<Command>,
    pub config: Arc<ConfigManager>,
}

impl Context {
    pub fn init(config: ConfigManager) -> Self {
        let (tx, _) = broadcast::channel::<Command>(8);
        Self { tx, config: Arc::new(config) }
    }

    pub fn config(&self) -> Arc<Config> {
        self.config.read()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Command> {
        self.tx.subscribe()
    }

    pub fn stop(&self) {
        let _ = self.tx.send(Command::Stop);
    }

    pub fn reload(&self) {
        let _ = self.tx.send(Command::Reload);
    }

    pub fn restart(&self) {
        let _ = self.tx.send(Command::Restart);
    }
}

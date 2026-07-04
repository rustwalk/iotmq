use crate::config::{Config, ConfigManager};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub enum ShutdownReason {
    Exit,
    Reload,
}

#[derive(Clone)]
pub struct Context {
    tx: broadcast::Sender<ShutdownReason>,
    config: Arc<ConfigManager>,
}

impl Context {
    pub fn init(config: ConfigManager) -> Self {
        let (tx, _) = broadcast::channel::<ShutdownReason>(8);
        Self { tx, config: Arc::new(config) }
    }

    pub fn config(&self) -> Arc<Config> {
        self.config.read()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ShutdownReason> {
        self.tx.subscribe()
    }

    pub fn exit(&self) {
        let _ = self.tx.send(ShutdownReason::Exit);
    }

    pub fn reload(&self) {
        let _ = self.tx.send(ShutdownReason::Reload);
    }
}

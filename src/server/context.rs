use crate::config::{Config, ConfigManager};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub enum Event {
    Stop,
    Reload,
    Restart,
}

#[derive(Clone)]
pub struct Context {
    tx: broadcast::Sender<Event>,
    pub config: Arc<ConfigManager>,
}

impl Context {
    pub fn init(config: ConfigManager) -> Self {
        let (tx, _) = broadcast::channel::<Event>(8);
        Self { tx, config: Arc::new(config) }
    }

    pub fn config(&self) -> Arc<Config> {
        self.config.read()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }

    pub fn stop(&self) {
        let _ = self.tx.send(Event::Stop);
    }

    pub fn reload(&self) {
        let _ = self.tx.send(Event::Reload);
    }

    pub fn restart(&self) {
        let _ = self.tx.send(Event::Restart);
    }
}

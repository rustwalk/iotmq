use crate::config::{Config, ConfigManager};
use std::cmp::PartialEq;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Running,
    Stop,
    Reload,
    Restart,
}

#[derive(Clone)]
pub struct Context {
    tx: watch::Sender<Event>,
    pub config: Arc<ConfigManager>,
}

impl Context {
    pub fn init(config: ConfigManager) -> Self {
        let (tx, _) = watch::channel(Event::Running);
        Self { tx, config: Arc::new(config) }
    }

    pub fn config(&self) -> Arc<Config> {
        self.config.read()
    }

    pub fn subscribe(&self) -> watch::Receiver<Event> {
        self.tx.subscribe()
    }

    pub fn stop(&self) {
        self.tx.send_replace(Event::Stop);
    }

    pub fn reload(&self) {
        self.tx.send_replace(Event::Reload);
    }

    pub fn restart(&self) {
        self.tx.send_replace(Event::Restart);
    }

    pub async fn shutdown(rx: &mut watch::Receiver<Event>) -> Event {
        loop {
            match rx.borrow_and_update().clone() {
                Event::Running | Event::Reload => {}
                event => return event,
            }

            if rx.changed().await.is_err() {
                return Event::Stop;
            }
        }
    }
}

use tokio::sync::broadcast;

#[derive(Clone)]
pub enum ShutdownReason {
    Exit,
    Reload
}

#[derive(Clone)]
pub struct Context {
    tx: broadcast::Sender<ShutdownReason>,
}

impl Context {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel::<ShutdownReason>(8);
        Self {
            tx
        }
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

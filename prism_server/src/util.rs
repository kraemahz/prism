use tokio::sync::mpsc;

pub struct ShutdownReceiver {
    rx: mpsc::UnboundedReceiver<()>,
}

impl ShutdownReceiver {
    pub async fn wait(mut self) {
        self.rx.recv().await;
    }
}

#[derive(Clone)]
pub struct ShutdownSender {
    tx: mpsc::UnboundedSender<()>,
}

impl ShutdownSender {
    pub fn signal(&self) {
        self.tx.send(()).ok();
    }
}

pub fn shutdown_channel() -> (ShutdownSender, ShutdownReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (ShutdownSender { tx }, ShutdownReceiver { rx })
}

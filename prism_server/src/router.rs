use std::sync::Arc;
use std::collections::HashSet;

use anymap::AnyMap;
use tokio::sync::mpsc;


#[derive(Hash)]
struct ChannelName(String);

#[derive(Hash)]
struct Channel {
}

pub struct Router {
    channels: AnyMap,
}

impl Router {
    pub fn new() -> Self {
        Self { channels: AnyMap::new() }
    }

    pub fn create_address<T: Send + Sync + 'static>(&mut self) -> mpsc::UnboundedReceiver<T> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.channels.insert(tx);
        rx
    }

    pub fn get_address<T: Send + Sync + 'static>(&self) -> Option<mpsc::UnboundedSender<T>> {
        self.channels.get::<mpsc::UnboundedSender<T>>().cloned()
    }
}


#[derive(Clone)]
pub struct BeamsTable {
    table: HashSet<Arc<str>>,
}


impl BeamsTable {
    pub fn new() -> Self {
        Self { table: HashSet::new() }
    }

    pub fn get_or_insert(&mut self, string: &str) -> Arc<str> {
        let string = Arc::from(string);
        match self.table.get(&string) {
            Some(str) => str.clone(),
            None => {
                self.table.insert(string.clone());
                string
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_router() {
        let rt = Runtime::new().expect("Failed to create a runtime");
        let mut router = Router::new();
        let mut rx = router.create_address::<()>();
        let tx = router.get_address::<()>().expect("Should get address");

        tx.send(()).unwrap();
        rt.block_on(rx.recv());
    }
}

use std::ffi::OsStr;
use std::fs::remove_file;
use std::path::{Path, PathBuf};

use capnp::message::{Builder, ReaderOptions};
use capnp_futures::{ReadStream, serialize};
use chrono::Utc;
use crc32fast::Hasher;
use log::warn;
use futures_util::StreamExt;
use tokio::fs::{File, OpenOptions, create_dir, read_dir};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::broadcast;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::log_capnp::disk_entry;


#[derive(Clone)]
pub struct Entry {
    pub index: u64,
    pub time: i64,
    pub payload: Vec<u8>,
}


pub async fn create_durable_queue(base_dir: &Path, queue_name: &str) -> Result<(DurableQueueWriter, DurableQueueReader), std::io::Error> {
    let mut queue_path = PathBuf::from(base_dir);
    queue_path.push(queue_name);
    create_dir(&queue_path).await?; 
    let (broadcast_tx, broadcast_rx) = broadcast::channel(1_000);

    let writer = DurableQueueWriter::create(queue_path.as_path(), broadcast_tx).await?;
    let reader = DurableQueueReader::new(queue_path.as_path(), broadcast_rx, 0, 0).await?;

    Ok((writer, reader))
}

pub struct DurableQueueWriter {
    base_dir: PathBuf,
    log_file: BufWriter<File>,

    index: u64,
    queue: broadcast::Sender<Entry>
}


const LOG_EXT: &str = "log";


impl Drop for DurableQueueWriter {
    fn drop(&mut self) {
        let lock_path = self.base_dir.join("lock");
        remove_file(lock_path).ok();
    }
}


impl DurableQueueWriter {
    /// Find the highest index in the directory if it exists.
    async fn find_max_index(path: &Path) -> Result<Option<u64>, std::io::Error> {
        let mut max_number: Option<u64> = None;
        let mut entries = read_dir(path).await?;

        while let Some(entry) = entries.next_entry().await? { 
            let path = entry.path();
            if path.is_file() {
                if let Some(file_stem) = path.file_stem().and_then(OsStr::to_str) {
                    if let Ok(number) = file_stem.parse::<u64>() {
                        max_number = Some(max_number.map_or(number, |max| max.max(number)));
                    }
                }
            }
        }

        Ok(max_number)
    }

    /// Function to read the last offset from the offset file
    async fn read_last_index(file: &mut File) -> Result<u64, std::io::Error> {
        let file = BufReader::new(file);
        let mut stream = ReadStream::new(file.compat(), ReaderOptions::new());
        let mut index = 0;

        while let Some(message_result) = stream.next().await {
            let message = match message_result {
                Ok(m) => m,
                Err(_) => continue,
            };
            let entry = message.get_root::<disk_entry::Reader>()
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to create message root"
                    )
                })?;
            index = entry.get_index();
        }

        Ok(index)
    }

    async fn write_lock(base_dir: &Path) -> Result<(), std::io::Error> {
        let lock_path = base_dir.join("lock");
        let mut lock_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(lock_path)
            .await
            .map_err(|_|
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "Could not create exclusive filesystem lock"))?;
        lock_file.write_u32(std::process::id()).await?;
        lock_file.flush().await?;
        Ok(())
    }

    /// Constructors
    pub async fn create(base_dir: &Path, queue: broadcast::Sender<Entry>) -> Result<Self, std::io::Error> {
        Self::write_lock(base_dir).await?;
        let log_path = base_dir.join(format!("0.{}", LOG_EXT));
        let log_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(log_path)
            .await?;
        let log_file = BufWriter::new(log_file);

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            log_file,
            index: 0,
            queue,
        })
    }

    pub async fn from_path(base_dir: &Path, queue: broadcast::Sender<Entry>) -> Result<Self, std::io::Error> {
        Self::write_lock(base_dir).await?;
        let current_segment = Self::find_max_index(base_dir).await?.unwrap_or(0);
        let log_path = base_dir.join(format!("{}.{}", current_segment, LOG_EXT));
        let mut log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(log_path)
            .await?;
        let index = Self::read_last_index(&mut log_file).await?;
        let log_file = BufWriter::new(log_file);

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            log_file,
            index,
            queue,
        })
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    async fn disk_push<'a>(&mut self, now: i64, payload: &[u8]) -> Result<(), std::io::Error> {
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let hash = hasher.finalize();

        let mut message = Builder::new_default();
        let mut entry = message.init_root::<disk_entry::Builder>();
        entry.set_index(self.index);
        entry.set_time(now);
        entry.set_hash(hash);
        entry.set_payload(payload);
        serialize::write_message((&mut self.log_file).compat_write(), &message)
            .await
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Serialization error: {:?}", e))
            })?;
        self.log_file.flush().await?;

        Ok(())
    }

    fn queue_push(&mut self, time: i64, payload: &[u8]) {
        self.queue.send(Entry{index: self.index, time, payload: payload.to_vec()}).ok();
    }

    pub async fn push(&mut self, payload: &[u8]) -> Result<(), std::io::Error> { 
        let now = Utc::now().timestamp();
        self.disk_push(now, payload).await?;
        self.queue_push(now, payload);
        self.index += 1;
        Ok(())
    }
}


pub struct DurableQueueReader {
    base_dir: PathBuf,
    log_file: Compat<BufReader<File>>,

    index: u64,
    memory_index: u64,
    queue: broadcast::Receiver<Entry>
}


impl DurableQueueReader {
    pub async fn new(base_dir: &Path,
                     queue: broadcast::Receiver<Entry>,
                     start_index: u64,
                     memory_index: u64) -> Result<Self, std::io::Error> {
        let current_segment = 0;  // TODO: Modify segment based on start index. 

        let mut log_path = PathBuf::from(base_dir);
        log_path.push(format!("{}.{}", current_segment, LOG_EXT));
        let log_file = File::open(&log_path).await?;
        let log_file = BufReader::new(log_file);

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            log_file: log_file.compat(),
            index: start_index,
            memory_index,
            queue
        })
    }

    pub async fn read_next(&mut self) -> Result<Entry, std::io::Error> {
        if self.index >= self.memory_index {
            let entry = self.queue.recv().await.map_err(|_|
                std::io::Error::new(std::io::ErrorKind::NotConnected,
                "Broadcast has closed"
            ))?;
            self.index = entry.index;
            return Ok(entry);
        }

        Ok(match self.read_from_log().await {
            Some(v) => v,
            None => {
                // Ran out of data, read from the memory queue.
                self.memory_index = self.index;
                let entry = self.queue.recv().await.map_err(|_|
                    std::io::Error::new(std::io::ErrorKind::NotConnected,
                    "Broadcast has closed"
                ))?;
                self.index = entry.index;
                entry
            }
        })
    }

    async fn read_from_log(&mut self) -> Option<Entry> {
        let message_result = serialize::read_message(
            &mut self.log_file,
            ReaderOptions::new()
        ).await;

        let reader = match message_result {
            Ok(r) => r,
            Err(_) => return None,
        };
        let message = reader.get_root::<disk_entry::Reader>().ok()?;

        let index = message.get_index();
        let time = message.get_time();
        let hash = message.get_hash();
        let payload = message.get_payload().ok()?;

        if hash != 0 {
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let new_hash  = hasher.finalize();
            if hash != new_hash {
                warn!("Hash failed for message in {:?} indexed {}", self.base_dir, index);
                return None;
            }
        }

        Some(Entry{
            index,
            time,
            payload: payload.to_vec()
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use tempfile::{tempdir, TempDir};

    fn setup() -> TempDir {
        tempdir().expect("Failed to assign temporary directory")
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        let rt = Runtime::new().expect("Failed to create a runtime");
        rt.block_on(future)
    }

    #[test]
    fn test_queue_creation() {
        let dir = setup();
        let base_dir = dir.path().to_path_buf();
        let (writer, _reader) = block_on(create_durable_queue(&base_dir, "test_queue")).expect("Failed to create queue");
        assert_eq!(writer.index(), 0);
        // Additional assertions to validate the state of the writer and reader can be added here.
    }

    #[test]
    fn test_push_and_read() {
        let dir = setup();
        let base_dir = dir.path().to_path_buf();
        let (mut writer, mut reader) = block_on(create_durable_queue(&base_dir, "test_queue")).expect("Failed to create queue");

        let payload1 = b"Hello, World! 1".to_vec();
        block_on(writer.push(&payload1)).expect("Failed to push to queue");
        let payload2 = b"Hello, World! 2".to_vec();
        block_on(writer.push(&payload2)).expect("Failed to push to queue");
        let payload3 = b"Hello, World! 3".to_vec();
        block_on(writer.push(&payload3)).expect("Failed to push to queue");

        let entry = block_on(reader.read_next()).expect("Failed to read from queue");
        assert_eq!(payload1, entry.payload);
        let entry = block_on(reader.read_next()).expect("Failed to read from queue");
        assert_eq!(payload2, entry.payload);
        let entry = block_on(reader.read_next()).expect("Failed to read from queue");
        assert_eq!(payload3, entry.payload);
    }
}

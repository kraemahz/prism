use std::ffi::OsStr;
use std::fs::remove_file;
use std::path::{Path, PathBuf};

use chrono::Utc;
use sha1::{Sha1, Digest};
use serde::{Serialize, Deserialize};
use tokio::fs::{File, OpenOptions, create_dir, read_dir};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt, SeekFrom, BufReader, BufWriter};


#[derive(Deserialize)]
struct QueueEntry {
    pub time: i64,
    pub payload: Vec<u8>,
    pub hash: [u8; 20],
}


#[derive(Serialize)]
struct QueueEntrySer<'a> {
    pub time: i64,
    pub payload: &'a[u8],
    pub hash: [u8; 20],
}

pub async fn create_durable_queue(base_dir: &Path, queue_name: &str) -> Result<(DurableQueueWriter, DurableQueueReader), std::io::Error> {
    let mut queue_path = PathBuf::from(base_dir);
    queue_path.push(queue_name);

    let writer = DurableQueueWriter::create(queue_path.as_path()).await?;
    let reader = DurableQueueReader::new(queue_path.as_path()).await?;

    Ok((writer, reader))
}

pub struct DurableQueueWriter {
    base_dir: PathBuf,
    log_file: BufWriter<File>,
    current_offset: u64,
    index: u64
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
    async fn read_last_offset(file: &mut File) -> Result<(u64, u64), std::io::Error> {
        let mut file = BufReader::new(file);
        let entry_size = std::mem::size_of::<u64>();
        let seek_from = -i64::try_from(entry_size).unwrap();

        // Get the offset from seeking to the end of the file.
        let offset = match file.seek(SeekFrom::End(seek_from)).await {
            // If seek fails its because we tried to index before the front.
            Err(_) => return Ok((0, 0)),
            Ok(sz) => sz + 8
        };
        let index = file.read_u64().await?;
        Ok((index, offset))
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
    pub async fn create(base_dir: &Path) -> Result<Self, std::io::Error> {
        create_dir(base_dir).await?; 
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
            current_offset: 0,
            index: 0,
        })
    }

    pub async fn from_path(base_dir: &Path) -> Result<Self, std::io::Error> {
        Self::write_lock(base_dir).await?;

        let current_segment = Self::find_max_index(base_dir).await?.unwrap_or(0);
        let log_path = base_dir.join(format!("{}.{}", current_segment, LOG_EXT));
        let mut log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(log_path)
            .await?;
        let (current_offset, index) = Self::read_last_offset(&mut log_file).await?;
        let log_file = BufWriter::new(log_file);

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            log_file,
            current_offset,
            index,
        })
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    async fn inner_push<'a>(&mut self, entry: &'a QueueEntrySer<'a>) -> Result<(), std::io::Error> {
        let serialized = serde_cbor::to_vec(entry).map_err(|e|
            std::io::Error::new(std::io::ErrorKind::Other,
                                format!("Serialization error: {:?}", e))
        )?;
        let len = serialized.len();

        self.current_offset += len as u64;

        let ser_len = len.to_be_bytes();
        let ser_index = self.index.to_be_bytes();

        self.log_file.write_all(&ser_len).await?;
        self.log_file.write_all(&serialized).await?;
        self.log_file.write_all(&ser_index).await?;
        self.log_file.flush().await?;

        self.index += 1;

        Ok(())
    }

    pub async fn push(&mut self, payload: &[u8]) -> Result<(), std::io::Error> { 
        let now = Utc::now().timestamp();
        let entry = QueueEntrySer{payload, hash: [0; 20], time: now};
        self.inner_push(&entry).await
    }

    pub async fn push_with_hash(&mut self, payload: &[u8]) -> Result<(), std::io::Error> {
        let now = Utc::now().timestamp();
        let mut hasher = Sha1::new();
        hasher.update(payload);
        let hash = hasher.finalize();
        let entry = QueueEntrySer{payload, hash: hash.into(), time: now};
        self.inner_push(&entry).await
    }
}


pub struct DurableQueueReader {
    log_file: BufReader<File>,

    index: u64,
    file_index: u64,
    last_read_offset: usize
}


impl DurableQueueReader {
    pub async fn new(base_dir: &Path) -> Result<Self, std::io::Error> {
        let current_segment = 0;  // TODO: Modify segment based on start index. 

        let mut log_path = PathBuf::from(base_dir);
        log_path.push(format!("{}.{}", current_segment, LOG_EXT));
        let log_file = File::open(&log_path).await?;
        let log_file = BufReader::new(log_file);

        Ok(Self {
            log_file,
            index: 0,
            file_index: 0,
            last_read_offset: 0
        })
    }

    async fn read_from_queue(&mut self) -> Result<Option<(u64, QueueEntry)>, std::io::Error> {
        let len = match self.log_file.read_u64().await {
            Ok(v) => v as usize,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
        };
        let mut contents = vec![0u8; len];
        self.log_file.read_exact(&mut contents).await?;
        let index = self.log_file.read_u64().await?;
        let entry_size = std::mem::size_of::<u64>();
        self.last_read_offset += entry_size * 2 + len;

        let entry: QueueEntry = serde_cbor::from_slice(&contents).map_err(|e|
            std::io::Error::new(std::io::ErrorKind::Other,
                                format!("DeserializationError: {:?}", e))
        )?;
        Ok(Some((index, entry)))
    }

    pub async fn read_next_verify_hash(&mut self) -> Result<Option<(i64, Vec<u8>)>, std::io::Error> {
        let (index, entry) = match self.read_from_queue().await? {
            Some(q) => q,
            None => return Ok(None)
        };
        self.index = index;

        if entry.hash != [0; 20] {
            let mut hasher = Sha1::new();
            hasher.update(&entry.payload);
            let hash: [u8; 20] = hasher.finalize().into();
            if entry.hash != hash {
                return Err(
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Invalid hash!"
                    )
                );
            }
        }

        Ok(Some((entry.time, entry.payload)))
    }

    pub async fn read_next(&mut self) -> Result<Option<(i64, Vec<u8>)>, std::io::Error> {
        let (index, entry) = match self.read_from_queue().await? {
            Some(q) => q,
            None => return Ok(None)
        };
        self.index = index;
        Ok(Some((entry.time, entry.payload)))
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

        let (_time, read_payload) = block_on(reader.read_next()).expect("Failed to read from queue").expect("No entry found");
        assert_eq!(payload1, read_payload);
        let (_time, read_payload) = block_on(reader.read_next()).expect("Failed to read from queue").expect("No entry found");
        assert_eq!(payload2, read_payload);
        let (_time, read_payload) = block_on(reader.read_next()).expect("Failed to read from queue").expect("No entry found");
        assert_eq!(payload3, read_payload);
    }

    #[test]
    fn test_push_with_hash_and_verify() {
        let dir = setup();
        let base_dir = dir.path().to_path_buf();
        let (mut writer, mut reader) = block_on(create_durable_queue(&base_dir, "test_push_with_hash_and_verify")).expect("Failed to create queue");

        let payload = b"Test Payload".to_vec();
        block_on(writer.push_with_hash(&payload)).expect("Failed to push to queue");

        let (_time, read_payload) = block_on(reader.read_next_verify_hash()).expect("Failed to read from queue").expect("No entry found");
        assert_eq!(payload, read_payload);
    }
}

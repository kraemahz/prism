use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, atomic::Ordering};

use async_recursion::async_recursion;
use globset::{Glob, GlobSetBuilder, GlobSet};
use tokio::task::JoinSet;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::{broadcast, mpsc, Mutex as AsyncMutex};

use crate::queue::{DurableQueueWriter, DurableQueueReader, Entry, NextError};
pub type ClientSender = mpsc::UnboundedSender<(Arc<str>, Entry)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub u64);

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct BeamServerHandle {
    beam_server: Arc<AsyncMutex<BeamServer>>
}

impl BeamServerHandle {
    pub async fn new(base_dir: PathBuf, table: &mut BeamsTable) -> Result<Self> {
        let beam_server = BeamServer::new(base_dir, table).await?;
        Ok(Self { beam_server: Arc::new(AsyncMutex::new(beam_server)) })
    }

    pub async fn list_beams(&self) -> Vec<Arc<str>> {
        let server = self.beam_server.lock().await;
        tracing::debug!("BeamServer::list_beams()");
        server.list_beams()
    }

    pub async fn new_writer(&self, client_id: ClientId, beam: &Arc<str>) -> Result<DurableQueueWriter> {
        let mut server = self.beam_server.lock().await;
        tracing::debug!("BeamServer::new_writer({:?})", beam);
        server.new_writer(client_id, beam).await
    }

    pub async fn drop_writer(&self, writer: DurableQueueWriter) {
        let mut server = self.beam_server.lock().await;
        tracing::debug!("BeamServer::drop_writer({:?})", writer.beam());
        server.drop_writer(writer);
    }

    pub async fn subscribe(&self, client_id: ClientId, beam: Arc<str>, index: Option<u64>) {
        let mut server = self.beam_server.lock().await;
        tracing::debug!("BeamServer::subscribe({:?}, {:?})", client_id, beam);
        server.subscribe(client_id, beam, index).await;
    }

    pub async fn unsubscribe(&self, client_id: ClientId, beam: Arc<str>) {
        let mut server = self.beam_server.lock().await;
        tracing::debug!("BeamServer::unsubscribe({:?}, {:?})", client_id, beam);
        server.unsubscribe(client_id, beam);
    }

    pub async fn add_client(&self, client_id: ClientId, sink: ClientSender) {
        let mut server = self.beam_server.lock().await;
        tracing::debug!("BeamServer::add_client({:?})", client_id);
        server.add_client(client_id, sink);
    }

    pub async fn drop_client(&self, client_id: ClientId) {
        let mut server = self.beam_server.lock().await;
        tracing::debug!("BeamServer::drop_client({:?})", client_id);
        server.drop_client(client_id);
    }
}


enum HandleUpdate {
    Senders(Vec<mpsc::UnboundedSender<(Arc<str>, Entry)>>)
}


#[derive(Debug)]
pub struct BeamReaderSet {
    beam: Arc<str>,
    writer_handles: HashMap<ClientId, (PathBuf, broadcast::Sender<Entry>, Arc<AtomicU64>)>,
    reader_handles: HashMap<ClientId, mpsc::Sender<HandleUpdate>>,
    subscriptions: HashMap<ClientId, ClientSender>
}


impl BeamReaderSet {
    pub fn new(beam: Arc<str>) -> Self {
        Self { beam,
               writer_handles: HashMap::new(),
               reader_handles: HashMap::new(),
               subscriptions: HashMap::new() }
    }

    async fn spawn_reader(&mut self, client_id: ClientId, index: u64) -> std::io::Result<()> {
        let (base_dir, queue, memory_index) = match self.writer_handles.get(&client_id) {
            Some(wh) => wh,
            None => return Err(
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Writer was not found"
                )
            )
        };
        let mut reader = DurableQueueReader::new(
            self.beam.clone(),
            base_dir.clone(),
            queue.subscribe(),
            index,
            memory_index.load(Ordering::Acquire)
        ).await?;
        let (tx, mut rx) = mpsc::channel(1);
        self.reader_handles.insert(client_id, tx);

        let beam = self.beam.clone();
        tracing::debug!("Reader task spawned: {:?} {}", client_id, beam);

        tokio::spawn(async move {
            let mut senders: Vec<ClientSender> = Vec::new();

            loop {
                let entry = if reader.cancel_safe() {
                    tokio::select!(
                        msg = reader.next() => msg,
                        update = rx.recv() => {
                            match update {
                                Some(HandleUpdate::Senders(v)) => {
                                    tracing::debug!("Senders update (select): {}", v.len());
                                    senders.clear();
                                    senders.extend(v);
                                }
                                None => break
                            }
                            continue;
                        }
                    )
                } else {
                    if let Ok(update) = rx.try_recv() {
                        match update {
                            HandleUpdate::Senders(v) => {
                                tracing::debug!("Senders update (interrupt): {}", v.len());
                                senders.clear();
                                senders.extend(v);
                            }
                        }
                    }
                    reader.next().await
                };

                match entry {
                    Ok(entry) => {
                        for send in senders.iter() {
                            send.send((beam.clone(), entry.clone())).ok();
                        }
                    }
                    Err(NextError::TryAgain) => continue,
                    Err(NextError::Closed) => {
                        tracing::info!("Writer stopped");
                        continue;
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn add_writer(&mut self, client_id: ClientId, writer: &DurableQueueWriter, index: u64) {
        let base_dir = writer.base_dir();
        let queue = writer.queue();
        let mem_index = writer.index();
        self.writer_handles.insert(client_id, (base_dir, queue, mem_index));

        if !self.subscriptions.is_empty() {
            if self.spawn_reader(client_id, index).await.is_err() {
                tracing::warn!("Reader task spawn failed");
                return;
            }
            let send_vec = self.subscriptions.values().cloned().collect();
            let reader_handle = self.reader_handles.get(&client_id).unwrap();
            reader_handle.send(HandleUpdate::Senders(send_vec)).await.ok();
        }
    }

    pub async fn subscribe(&mut self, client_id: ClientId, sender: ClientSender, index: Option<u64>) {
        let was_empty = self.subscriptions.is_empty();
        self.subscriptions.insert(client_id, sender);
        if was_empty {
            tracing::info!("Subscribe starting new tasks (was_empty)");
            match index {
                Some(index) => {
                    let client_ids: Vec<_> = self.writer_handles.keys().copied().collect();
                    for write_client_id in client_ids {
                        if self.spawn_reader(write_client_id, index).await.is_err() {
                            tracing::warn!("Reader task spawn failed");
                            return;
                        }
                    }
                }
                None => {
                    let clients: Vec<_> = self.writer_handles.iter().map(|(w, (_, _, idx))| (*w, idx.clone())).collect();
                    for (write_client_id,  memory_index) in clients {
                        let index = memory_index.load(Ordering::Relaxed);
                        if self.spawn_reader(write_client_id, index).await.is_err() {
                            tracing::warn!("Reader task spawn failed");
                            return;
                        }
                    }
                }
            }
        }

        let send_vec: Vec<_> = self.subscriptions.values().cloned().collect();
        for reader_handle in self.reader_handles.values() {
            reader_handle.send(HandleUpdate::Senders(send_vec.clone())).await.ok();
        }
    }

    pub fn unsubscribe(&mut self, client_id: ClientId) {
        self.subscriptions.remove(&client_id);
        if self.subscriptions.is_empty() {
            self.reader_handles.clear();
        }
    }
}


#[derive(Debug)]
pub struct BeamServer {
    base_dir: PathBuf,
    beams: HashMap<Arc<str>, Beam>,
    clients: HashMap<ClientId, mpsc::UnboundedSender<(Arc<str>, Entry)>>,
    readers: HashMap<Arc<str>, BeamReaderSet>
}

impl BeamServer {
    pub async fn new(base_dir: PathBuf, table: &mut BeamsTable) -> Result<Self> {
         Ok(Self { base_dir: base_dir.clone(),
                   beams: setup_queues(base_dir, table).await?,
                   clients: HashMap::new(),
                   readers: HashMap::new() })
    }

    pub fn list_beams(&self) -> Vec<Arc<str>> {
        self.beams.keys().cloned().collect()
    }

    pub async fn new_writer(&mut self, client_id: ClientId, beam_name: &Arc<str>) -> Result<DurableQueueWriter> {
       let beam = match self.beams.get_mut(beam_name) {
           Some(beam) => beam,
           None => {
               let beam = match Beam::load(&self.base_dir, beam_name.clone()).await {
                   Ok(beam) => beam,
                   Err(err) => {
                       return Err(err);
                   }
               };
               self.beams.insert(beam_name.clone(), beam);
               self.beams.get_mut(beam_name).unwrap()
           }
       };
       let writer = match beam.next_writer().await {
           Ok(writer) => writer,
           Err(err) => {
               return Err(err);
           }
       };
       let beam_set = self.readers.entry(beam_name.clone())
            .or_insert_with(|| BeamReaderSet::new(beam_name.clone()));
       beam_set.add_writer(client_id, &writer, 0).await;
       Ok(writer)
    }

    pub fn drop_writer(&mut self, writer: DurableQueueWriter) {
       let beam_name = writer.beam();
       let beam = match self.beams.get_mut(&beam_name) {
           Some(beam) => beam,
           None => {
               tracing::error!("Writer sent for non-existant beam: {}", beam_name);
               return;
           }
       };
       beam.reclaim(writer);
    }

    pub fn add_client(&mut self, client_id: ClientId, sink: mpsc::UnboundedSender<(Arc<str>, Entry)>) {
        self.clients.insert(client_id, sink);
    }

    pub fn drop_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);
    }

    pub async fn subscribe(&mut self, client_id: ClientId, beam: Arc<str>, index: Option<u64>) {
        let sender = match self.clients.get(&client_id) {
            Some(sender) => sender.clone(),
            None => {
                tracing::error!("No sender for client {:?}", client_id);
                return;
            }
        };
        let beam_set = self.readers.entry(beam.clone())
            .or_insert_with(|| BeamReaderSet::new(beam));
        beam_set.subscribe(client_id, sender, index).await;
    }

    pub fn unsubscribe(&mut self, client_id: ClientId, beam: Arc<str>) {
        let beam_set = match self.readers.get_mut(&beam) {
            Some(beam_set) => beam_set,
            None => return
        };
        beam_set.unsubscribe(client_id);
    }
}


#[derive(Debug)]
pub struct Beam {
    base_dir: PathBuf,
    beam: Arc<str>,
    writer_paths: Vec<PathBuf>,
    inactive_writers: Vec<DurableQueueWriter>,
}


impl Drop for Beam {
    fn drop(&mut self) {
        self.dump_to_base_dir().ok();
    }
}


impl Beam {
    const PARTITION_FILE: &'static str = "partitions";

    async fn load_base_dir(base_dir: &PathBuf, beam: Arc<str>) -> Result<Vec<DurableQueueWriter>> {
        let mut partition = base_dir.clone();
        partition.push(Self::PARTITION_FILE);

        let dirs: Vec<PathBuf> = match File::open(partition).await {
            Ok(mut file) => {
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).await?;
                serde_json::from_slice(&buffer).map_err(|e|
                    Error::new(ErrorKind::Other, e.to_string()))?
            }
            Err(_) => Vec::new(),
        };

        let mut queues = Vec::new();
        for dir in dirs {
            let mut queue_dir = base_dir.clone();
            queue_dir.push(&dir);
            let queue = match DurableQueueWriter::from_path(queue_dir.clone(), beam.clone()).await {
                Ok(queue) => queue,
                Err(_) => {
                    tracing::error!("Missing queue entry: {:?}", queue_dir);
                    continue
                }
            };
            queues.push(queue);
        }
        Ok(queues)
    }

    fn dump_to_base_dir(&self) -> Result<()> {
        use std::io::Write;
        let mut partition = self.base_dir.clone();
        partition.push(Self::PARTITION_FILE);
        let mut file = std::fs::File::create(partition)?;
        let mut buffer = serde_json::to_vec(&self.writer_paths)
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        file.write_all(&mut buffer)?;
        Ok(())
    }

    pub async fn load(base_dir: &PathBuf, beam: Arc<str>) -> Result<Self> {
        let inactive_writers = Self::load_base_dir(base_dir, beam.clone()).await?;
        let paths: Vec<PathBuf> = inactive_writers.iter()
            .map(|w| {
                PathBuf::from(w.base_dir().file_stem().unwrap())
            })
            .collect();

        Ok(Self {
            base_dir: base_dir.clone(),
            beam,
            writer_paths: paths,
            inactive_writers,
        })
    }

    pub async fn next_writer(&mut self) -> Result<DurableQueueWriter> {
        match self.inactive_writers.pop() {
            Some(w) => Ok(w),
            None => {
                let w = DurableQueueWriter::create_by_beam(
                    self.base_dir.as_path(),
                    self.beam.clone(),
                    self.writer_paths.len() as u64
                ).await?;
                let path = PathBuf::from(w.base_dir().as_path().file_stem().unwrap());
                self.writer_paths.push(path.to_path_buf());
                Ok(w)
            }
        }
    }

    pub fn reclaim(&mut self, writer: DurableQueueWriter) {
        self.inactive_writers.push(writer);
    }
}


#[async_recursion]
async fn walk_with_glob(dir: PathBuf, pattern: GlobSet) -> Vec<PathBuf> {
    let mut dirs = vec![];

    let mut read_dir = match tokio::fs::read_dir(dir).await {
        Ok(read_dir) => read_dir,
        Err(_) => return dirs,
    };

    let mut paths = vec![];
    let mut joins = JoinSet::new();

    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        if path.is_dir() {
            paths.push((path.clone(), pattern.clone()));
        } else if pattern.is_match(&path) {
            dirs.push(path.parent().unwrap().to_path_buf());
        }
    }

    for (path, pattern) in paths {
        joins.spawn(walk_with_glob(path, pattern));
    }
    while let Some(res) = joins.join_next().await {
        let paths = res.unwrap();
        dirs.extend(paths);
    }

    dirs
}


pub async fn setup_queues(base_dir: PathBuf, table: &mut BeamsTable) -> Result<HashMap<Arc<str>, Beam>> {
    let mut queues = HashMap::new();

    let mut glob_builder = GlobSetBuilder::new();
    glob_builder.add(Glob::new(&format!("**/{}", Beam::PARTITION_FILE)).unwrap());
    let glob = glob_builder.build().unwrap();

    let dirs = walk_with_glob(base_dir.clone(), glob).await;
    for dir in dirs {
        let dir_str = dir.file_name().unwrap().to_ascii_lowercase();
        let beam_str = dir_str.to_str().unwrap_or("default");
        let beam_str = table.get_or_insert(beam_str);
        let beam_base = dir.parent().unwrap();
        let beam = match Beam::load(&beam_base.to_path_buf(), beam_str.clone()).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Beam failed to load: {:?}", e);
                continue;
            }
        };
        queues.insert(beam_str, beam);
    }

    Ok(queues)
}

use std::path::PathBuf;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};

use async_recursion::async_recursion;
use globset::{Glob, GlobSetBuilder, GlobSet};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinSet;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Mutex as AsyncMutex};

use crate::exchange::ClientId;
use crate::router::BeamsTable;
use crate::queue::{Entry, DurableQueueWriter, DurableQueueReader};

#[derive(Clone,Debug)]
pub struct BeamServerHandle {
    beam_server: Arc<Mutex<BeamServer>>
}

impl BeamServerHandle {
    pub async fn new(base_dir: PathBuf, table: &mut BeamsTable) -> Result<Self> {
        let beam_server = BeamServer::new(base_dir, table).await?;
        Ok(Self { beam_server: Arc::new(Mutex::new(beam_server)) })
    }

    pub async fn new_writer(&self, beam: &Arc<str>) -> Result<DurableQueueWriter> {
        let mut server = self.beam_server.lock().unwrap();
        server.new_writer(beam).await
    }

    pub async fn drop_writer(&self, writer: DurableQueueWriter) {
        let mut server = self.beam_server.lock().unwrap();
        server.drop_writer(writer).await;
    }

    pub fn add_client(&self, client_id: ClientId, sink: mpsc::UnboundedSender<(Arc<str>, Entry)>) {
        let mut server = self.beam_server.lock().unwrap();
        server.add_client(client_id, sink);
    }

    pub fn drop_client(&self, client_id: ClientId) {
        let mut server = self.beam_server.lock().unwrap();
        server.drop_client(client_id);
    }
}


pub enum BeamMessage {
    NewWriter(Arc<str>),
    Writer(DurableQueueWriter)
}


pub enum BeamResponse {
    Writer(DurableQueueWriter)
}


#[derive(Debug)]
pub struct BeamServer {
    base_dir: PathBuf,
    beams: HashMap<Arc<str>, Beam>,
    clients: HashMap<ClientId, UnboundedSender<(Arc<str> Entry)>>,
}

impl BeamServer {
    pub async fn new(base_dir: PathBuf, table: &mut BeamsTable) -> Result<Self> {
         Ok(Self { base_dir: base_dir.clone(),
                   beams: setup_queues(base_dir, table).await?,
                   clients: HashMap::new() })
    }

    pub async fn new_writer(&mut self, beam_name: &Arc<str>) -> Result<DurableQueueWriter> {
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
       Ok(writer)
    }

    pub async fn drop_writer(&mut self, writer: DurableQueueWriter) {
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

    pub fn add_client(&self, client_id: ClientId, sink: mpsc::UnboundedSender<(Arc<str>, Entry)>) {
        self.clients.insert(client_id, sink);
    }

    pub fn drop_client(&self, client_id: ClientId) {
        self.clients.remove(&client_id);
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
            let queue = match DurableQueueWriter::from_path(queue_dir, beam.clone()).await {
                Ok(queue) => queue,
                Err(_) => {
                    tracing::error!("Missing queue entry: {:?}", dir);
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


pub async fn beam_join(
    mut reader: DurableQueueReader,
    beam_messages: Arc<AsyncMutex<HashMap<ClientId, mpsc::UnboundedSender<(Arc<str>, Entry)>>>>,
    stop: Arc<AtomicBool>,
) {
    let beam = reader.beam();
    loop {
        {
            let beam_messages = beam_messages.lock().await;
            while !stop.load(Ordering::Relaxed) {
                if let Some(entry) = reader.next().await {
                    let mut should_break = false;
                    for beam_message in beam_messages.values() {
                        if beam_message.send((beam.clone(), entry.clone())).is_err() {
                            // Kick back out, this channel closed and should be refreshed
                            should_break = true;
                        }
                    }
                    if should_break {
                        break
                    }
                }
            }
        }

        // Here we wait for the bool to be reset by the other side when it's done.
        while stop.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
    }
}

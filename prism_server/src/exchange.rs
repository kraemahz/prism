use std::collections::{HashMap, hash_map::Entry as HashEntry};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::Result;

use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio::task::{JoinHandle, JoinSet};

use crate::queue::{
    Entry,
    Photon,
    DurableQueueWriter,
    DurableQueueReader,
};
use crate::beam::{Beam, beam_join, setup_queues};
use crate::router::{BeamsTable, Router};
use crate::util::ShutdownSender;
use prism_schema::RequestType;


#[derive(Debug)]
pub enum BeamRequest {
    Init,
    List,
    Subscribe(ClientId, Arc<str>, Option<u64>),
    Unsubscribe(ClientId, Arc<str>),
    Enable(ClientId, Arc<str>)
}

impl BeamRequest {
    pub fn from(client: ClientId,
                beams_table: &Arc<Mutex<BeamsTable>>,
                rtype: RequestType) -> Self {
        let mut table = beams_table.lock().unwrap();

        match rtype {
            RequestType::AddBeam(beam) => {
                let handle = table.get_or_insert(&beam);
                Self::Enable(client, handle)
            }
            RequestType::ListBeams => Self::List,
            RequestType::Subscribe(beam, index) => {
                let handle = table.get_or_insert(&beam);
                Self::Subscribe(client, handle, index)
            }
            RequestType::Unsubscribe(beam) => {
                let handle = table.get_or_insert(&beam);
                Self::Unsubscribe(client, handle)
            }
        }
    }
}

pub enum BeamResponse {
    Init(ClientId, mpsc::Sender<Photon>, mpsc::UnboundedReceiver<(Arc<str>, Entry)>),
    List(Vec<String>),
}

pub type BeamChannel = (BeamRequest, oneshot::Sender<BeamResponse>);
pub const CHANNEL_SIZE: usize = 1_000;


#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub u64);

type ClientSenderCollection =
    HashMap<Arc<str>, (Arc<AsyncMutex<HashMap<ClientId, mpsc::UnboundedSender<(Arc<str>, Entry)>>>>,
                       Arc<AtomicBool>)>;
type WriteClientCollection = HashMap<ClientId, HashMap<Arc<str>, DurableQueueWriter>>;


fn new_client(
    client_id: ClientId,
    base_dir: PathBuf,
    writers: Arc<AsyncMutex<HashMap<Arc<str>, Beam>>>,
    write_clients: Arc<AsyncMutex<WriteClientCollection>>,
    mut client_rx: mpsc::Receiver<Photon>,
) {
    tokio::spawn(async move {
        while let Some(photon) = client_rx.recv().await {
            let Photon{beam: beam_name, payload} = photon;

            let mut all_write_clients = write_clients.lock().await;
            let write_clients = all_write_clients.entry(client_id)
                .or_insert_with(HashMap::new);

            if !write_clients.contains_key(&beam_name) {
                tracing::warn!("{:?} took the slow path on emit of {}, consider using enable",
                               client_id,
                               beam_name);
                let mut writers = writers.lock().await;

                if !writers.contains_key(&beam_name) {
                    let beam = match Beam::load(&base_dir, beam_name.clone()).await {
                        Ok(b) => b,
                        Err(err) => {
                            tracing::error!("DROPPED PACKET: Failed to create Beam {:?} at {:?}: {:?}",
                                            beam_name, base_dir, err);
                            continue;
                        }
                    };
                    writers.insert(beam_name.clone(), beam);
                }
                let beam = writers.get_mut(&beam_name).unwrap();
                let writer = match beam.next_writer().await {
                    Ok(dqw) => dqw,
                    Err(err) => {
                        tracing::error!("DROPPED PACKET: Failed to create new writer for beam {:?} at {:?}: {:?}",
                                        beam_name, base_dir, err);
                        continue;
                    }
                };
                write_clients.insert(beam_name.clone(), writer);
            }

            let writer = write_clients.get_mut(&beam_name).unwrap();
            if let Err(err) = writer.push(&payload).await {
                tracing::error!("DROPPED PACKET: Error pushing to writer: {:?}", err);
                write_clients.remove(&beam_name);
            };
            tracing::debug!("Photon sent");
        }

        let mut writers = writers.lock().await;
        let mut all_write_clients = write_clients.lock().await;
        let write_clients = all_write_clients.entry(client_id)
            .or_insert_with(HashMap::new);
        for (beam, writer) in write_clients.drain() {
            if let Some(beam) = writers.get_mut(&beam) {
                beam.reclaim(writer);
            }
        }

    });
}


async fn new_readers_on_beam(
    write_clients: Arc<AsyncMutex<WriteClientCollection>>,
    beam_name: Arc<str>,
    index: Option<u64>,
) -> Vec<DurableQueueReader> {
    let mut readers = vec![];
    let write_clients = write_clients.lock().await;
    let mut joins = JoinSet::new();

    for client_group in write_clients.values() {
        if let Some(queue) = client_group.get(&beam_name) {
            // Convince Rust the reference will exist while the task is running.
            let queue_ref: &'static DurableQueueWriter = unsafe { std::mem::transmute(queue) };
            joins.spawn(queue_ref.subscribe(index));
        }
    }
    while let Some(res) = joins.join_next().await {
        if let Ok(Ok(reader)) = res {
            readers.push(reader);
        }
    }

    readers
}





pub async fn run_queues(base_dir: PathBuf,
                        router: Arc<Mutex<Router>>,
                        beams_table: Arc<Mutex<BeamsTable>>,
                        shutdown: ShutdownSender) -> Result<JoinHandle<Result<()>>> {
    let mut beams_rx = router.lock().unwrap().create_address::<BeamChannel>();
    let mut table = beams_table.lock().unwrap();
    let beams: Arc<AsyncMutex<HashMap<Arc<str>, Beam>>> =
        setup_queues(base_dir.clone(), &mut table).await?;

    Ok(tokio::spawn(async move {
        let write_clients = Arc::new(AsyncMutex::new(HashMap::new()));
        let reader_queues: Arc<AsyncMutex<ClientSenderCollection>> =
            Arc::new(AsyncMutex::new(HashMap::new()));

        tracing::debug!("Handle request");
        while let Some((request, channel)) = beams_rx.recv().await {
            match request {
                BeamRequest::Init => {
                    let client_id = ClientId(clients.len() as u64);
                    let (client_tx, client_rx) = mpsc::channel(CHANNEL_SIZE);
                    let (server_tx, server_rx) = mpsc::unbounded_channel();
                    clients.insert(client_id, server_tx);

                    tracing::debug!("Init response");
                    channel.send(BeamResponse::Init(client_id, client_tx, server_rx)).ok();
                    new_client(client_id, base_dir.clone(), beams.clone(), write_clients.clone(), client_rx);
                }
                BeamRequest::Enable(client_id, beam_name) => {
                    let mut all_write_clients = write_clients.lock().await;
                    let write_clients = all_write_clients.entry(client_id)
                        .or_insert_with(HashMap::new);
                    if !write_clients.contains_key(&beam_name) {
                        let mut writers = beams.lock().await;

                        if !writers.contains_key(&beam_name) {
                            let beam = match Beam::load(&base_dir, beam_name.clone()).await {
                                Ok(b) => b,
                                Err(_) => continue,
                            };
                            writers.insert(beam_name.clone(), beam);
                        }
                        let beam = writers.get_mut(&beam_name).unwrap();
                        let writer = match beam.next_writer().await {
                            Ok(dqw) => dqw,
                            Err(_) => continue,
                        };
                        write_clients.insert(beam_name.clone(), writer);
                    }
                }
                BeamRequest::List=> {
                    let queues = beams.lock().await;
                    tracing::debug!("List response");
                    channel.send(BeamResponse::List(
                        queues.keys().map(|s| String::from(s.as_ref())).collect())
                    ).ok();
                }
                BeamRequest::Subscribe(client_id, beam_name, start_index) => {
                    let sender = match clients.get(&client_id) {
                        Some(s) => s,
                        None => continue,
                    };

                    let mut readers = reader_queues.lock().await;

                    match readers.entry(beam_name.clone()) {
                        HashEntry::Occupied(r) => {
                            let (messages, stop) = r.get();
                            stop.store(true, Ordering::Relaxed);
                            let mut messages = messages.lock().await;
                            messages.insert(client_id, sender.clone());
                            stop.store(false, Ordering::Relaxed);
                        }
                        HashEntry::Vacant(inserter) => {
                            let readers = new_readers_on_beam(
                                write_clients.clone(),
                                beam_name.clone(),
                                start_index).await;

                            let messages = Arc::new(AsyncMutex::new(HashMap::new()));
                            {
                                let mut m = messages.lock().await;
                                m.insert(client_id, sender.clone());
                            }
                            let stop = Arc::new(AtomicBool::new(false));
                            for reader in readers {
                                let messages = messages.clone();
                                let stop = stop.clone();
                                tokio::spawn(beam_join(reader, messages, stop));
                            }
                            inserter.insert((messages, stop));
                        }
                    };
                }
                BeamRequest::Unsubscribe(client_id, beam_name) => {
                    let readers = reader_queues.lock().await;
                    if let Some((messages, stop)) = readers.get(&beam_name) {
                        stop.store(true, Ordering::Relaxed);
                        let mut messages = messages.lock().await;
                        messages.remove(&client_id);
                        stop.store(false, Ordering::Relaxed);
                    }
                }
            }
        }
        tracing::info!("Queue exchange exiting");
        shutdown.signal();
        Ok(())
    }))
}

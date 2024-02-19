use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use futures_util::{
    stream::{SplitSink, SplitStream, StreamExt},
    SinkExt,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_websockets::{Error, Message, ServerBuilder, WebsocketStream};

use crate::beam::{BeamServerHandle, BeamsTable, ClientId};
use crate::queue::{DurableQueueWriter, Entry, Photon};
use crate::util::ShutdownSender;
use prism_schema::{
    pubsub::{client_greeting, client_message, server_greeting, server_message},
    ClientRequest, RequestType, ResponseType, ServerResponse,
};

fn write_to_message(message: Builder<HeapAllocator>) -> Message {
    let mut buffer: Vec<u8> = Vec::new();
    capnp::serialize::write_message(&mut buffer, &message).expect("Couldn't allocate memory"); // BUG potential
    let bytes = BytesMut::from(&buffer[..]);
    Message::binary(bytes)
}

pub fn entry_to_message(entries: &HashMap<Arc<str>, Vec<Entry>>) -> Message {
    let mut message = Builder::new(HeapAllocator::new());
    let mut server_msg = message.init_root::<server_message::Builder>();
    let mut beams = server_msg.reborrow().init_beams(entries.len() as u32);

    for (i, (beam_name, photons)) in entries.iter().enumerate() {
        let mut beam = beams.reborrow().get(i as u32);
        beam.reborrow()
            .set_name(capnp::text::Reader::from(beam_name.as_ref()));
        let mut photons_writer = beam.reborrow().init_photons(photons.len() as u32);
        for (j, photon) in photons.iter().enumerate() {
            let mut photon_writer = photons_writer.reborrow().get(j as u32);
            photon_writer.set_index(photon.index);
            photon_writer.set_time(photon.time);
            photon_writer.set_payload(&photon.payload);
        }
    }
    write_to_message(message)
}

const WS_LIMIT: usize = 100;

async fn send_events_task(
    mut entry_rx: mpsc::UnboundedReceiver<(Arc<str>, Entry)>,
    ws_tx: mpsc::Sender<Message>,
) {
    let mut messages = Vec::with_capacity(WS_LIMIT);
    while entry_rx.recv_many(&mut messages, WS_LIMIT).await > 0 {
        send_events(&mut messages, &ws_tx).await;
    }
}

#[tracing::instrument]
async fn send_events(messages: &mut Vec<(Arc<str>, Entry)>, ws_tx: &mpsc::Sender<Message>) {
    let mut hash: HashMap<Arc<str>, Vec<Entry>> = HashMap::new();
    for (beam_name, entry) in messages.drain(..) {
        hash.entry(beam_name).or_default().push(entry);
    }
    let message = entry_to_message(&hash);
    ws_tx.send(message).await.ok();
}

async fn send_messages_task(
    client_addr: String,
    mut ws_sink: SplitSink<WebsocketStream<TcpStream>, Message>,
    mut ws_rx: mpsc::Receiver<Message>,
) {
    while let Some(message) = ws_rx.recv().await {
        if ws_sink.send(message).await.is_err() {
            break;
        }
    }
    tracing::info!("Client {} stream disconnected", client_addr);
}

#[inline]
fn get_handle(beam_name: String, beams_table: &Arc<Mutex<BeamsTable>>) -> Arc<str> {
    let mut table = beams_table.lock().unwrap();
    table.get_or_insert(&beam_name)
}

#[tracing::instrument]
#[inline]
async fn send_entry(
    client_id: ClientId,
    entry: Photon,
    beam_server: &BeamServerHandle,
    writers: &mut HashMap<Arc<str>, DurableQueueWriter>,
) {
    if let Some(writer) = writers.get_mut(&entry.beam) {
        writer.push(&entry.payload).await.ok();
        return;
    };

    // No writer is found, take the slow path.
    tracing::warn!(
        "{:?} took the slow path on emit of {}, consider using enable",
        client_id,
        entry.beam
    );

    let mut fetch_writer = match beam_server.new_writer(client_id, &entry.beam).await {
        Ok(w) => w,
        Err(err) => {
            tracing::error!(
                "DROPPED PACKET: {:?} failed to create new writer for beam {:?}: {:?}",
                client_id,
                entry.beam,
                err
            );
            return;
        }
    };
    fetch_writer.push(&entry.payload).await.ok();
    writers.insert(entry.beam.clone(), fetch_writer);
}

#[tracing::instrument]
#[inline]
async fn handle_request(
    client_id: ClientId,
    request: ClientRequest,
    beams_table: &Arc<Mutex<BeamsTable>>,
    beam_server: &BeamServerHandle,
    writers: &mut HashMap<Arc<str>, DurableQueueWriter>,
    ws_tx: &mpsc::Sender<Message>,
) {
    let ClientRequest {
        id: request_id,
        rtype,
    } = request;
    let response_type = match rtype {
        RequestType::ListBeams => {
            tracing::info!("ListBeams {:?}", client_id);
            let beams = beam_server.list_beams().await;
            let beams: Vec<_> = beams.into_iter().map(|b| b.to_string()).collect();
            ResponseType::Beams(beams)
        }
        RequestType::AddBeam(beam) => {
            let beam = {
                let mut table = beams_table.lock().unwrap();
                table.get_or_insert(&beam)
            };

            match beam_server.new_writer(client_id, &beam).await {
                Ok(fetch_writer) => {
                    tracing::info!("Add beam: {}", beam);
                    writers.insert(beam.clone(), fetch_writer);
                    ResponseType::Ack
                }
                Err(err) => ResponseType::Error(err.to_string()),
            }
        }
        RequestType::Subscribe(beam, index) => {
            tracing::info!("Subscribe {:?}: {}", client_id, beam);
            let beam = get_handle(beam, beams_table);
            beam_server.subscribe(client_id, beam, index).await;
            ResponseType::Ack
        }
        RequestType::Unsubscribe(beam) => {
            tracing::info!("Unsubscribe {:?}: {}", client_id, beam);
            let beam = get_handle(beam, beams_table);
            beam_server.unsubscribe(client_id, beam).await;
            ResponseType::Ack
        }
    };

    let response = ServerResponse {
        id: request_id,
        rtype: response_type,
    };
    tracing::debug!("{:?}", response);
    let string = serde_json::to_string(&response).unwrap();
    let message = Message::text(string);
    ws_tx.send(message).await.unwrap();
}

async fn handle_client_message_task(
    client_id: ClientId,
    mut ws_source: SplitStream<WebsocketStream<TcpStream>>,
    beam_server: BeamServerHandle,
    beams_table: Arc<Mutex<BeamsTable>>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<(), Error> {
    let mut writers = HashMap::new();

    while let Some(Ok(msg)) = ws_source.next().await {
        if msg.is_binary() {
            let buf = msg.into_payload();
            let bytes = buf.to_vec();
            let reader_result =
                capnp::serialize::read_message(bytes.as_slice(), ReaderOptions::new());
            let reader = reader_result.unwrap();
            let msg = reader.get_root::<client_message::Reader>().unwrap();
            let emission = msg.get_emission().unwrap();

            let beam = emission.get_beam().unwrap().to_string().unwrap();
            let beam = get_handle(beam, &beams_table);
            let payload = emission.get_payload().unwrap().to_vec();
            tracing::trace!("Entry({})", beam);
            let entry = Photon { beam, payload };
            send_entry(client_id, entry, &beam_server, &mut writers).await;
        } else if msg.is_text() {
            let client_request: ClientRequest = match serde_json::from_str(msg.as_text().unwrap()) {
                Ok(r) => r,
                Err(_) => {
                    tracing::info!("Invalid message sent by {:?}", client_id);
                    break;
                }
            };
            tracing::debug!("{:?}", client_request);
            handle_request(
                client_id,
                client_request,
                &beams_table,
                &beam_server,
                &mut writers,
                &ws_tx,
            )
            .await;
        } else if msg.is_ping() {
            let payload = msg.into_payload();
            let bytes_mut = BytesMut::from(payload.as_ref());
            let pong = Message::pong(bytes_mut);
            ws_tx.send(pong).await.unwrap();
        }
    }
    Ok(())
}

async fn read_greeting(
    ws_source: &mut SplitStream<WebsocketStream<TcpStream>>,
) -> Result<u64, Error> {
    if let Some(msg) = ws_source.next().await {
        let msg = msg?;
        let buf = msg.into_payload();
        let bytes = buf.to_vec();
        let reader_result = capnp::serialize::read_message(bytes.as_slice(), ReaderOptions::new());
        let reader = reader_result.unwrap();
        let msg = reader.get_root::<client_greeting::Reader>().unwrap();
        let id = msg.get_id();

        return Ok(id);
    }
    Err(Error::AlreadyClosed)
}

async fn send_server_greeting(
    ws_sink: &mut SplitSink<WebsocketStream<TcpStream>, Message>,
    client_id: ClientId,
) -> Result<(), Error> {
    let message = {
        let mut message = Builder::new(HeapAllocator::new());
        let mut server_msg = message.init_root::<server_greeting::Builder>();
        server_msg.set_id(client_id.0);
        write_to_message(message)
    };
    ws_sink.send(message).await?;

    Ok(())
}

pub async fn init_client(
    sent_id: u64,
    clients: &mut HashSet<u64>,
    beam_server: &BeamServerHandle,
) -> (ClientId, mpsc::UnboundedReceiver<(Arc<str>, Entry)>) {
    let client_id = if clients.contains(&sent_id) {
        ClientId(sent_id)
    } else {
        let rn = rand::random::<u64>();
        clients.insert(rn);
        ClientId(rn)
    };
    let (server_tx, server_rx) = mpsc::unbounded_channel();
    beam_server.add_client(client_id, server_tx).await;
    (client_id, server_rx)
}

pub async fn run_web_server(
    addr: &str,
    beams_table: Arc<Mutex<BeamsTable>>,
    beam_server: BeamServerHandle,
    shutdown: ShutdownSender,
) -> Result<JoinHandle<Result<(), Error>>, Error> {
    let listener = TcpListener::bind(addr).await?;

    Ok::<_, Error>(tokio::spawn(async move {
        let mut clients: HashSet<u64> = HashSet::new();

        while let Ok((stream, client_addr)) = listener.accept().await {
            let ws_stream = ServerBuilder::new().accept(stream).await?;
            let (mut ws_sink, mut ws_source) = ws_stream.split();

            let sent_id = read_greeting(&mut ws_source).await?;
            let (client_id, entry_rx) = init_client(sent_id, &mut clients, &beam_server).await;
            send_server_greeting(&mut ws_sink, client_id).await?;

            let (ws_tx, ws_rx) = mpsc::channel::<Message>(WS_LIMIT);

            tokio::spawn(send_messages_task(client_addr.to_string(), ws_sink, ws_rx));
            tokio::spawn(send_events_task(entry_rx, ws_tx.clone()));

            let beams_table = beams_table.clone();
            tokio::spawn(handle_client_message_task(
                client_id,
                ws_source,
                beam_server.clone(),
                beams_table,
                ws_tx,
            ));
        }
        tracing::info!("Web server exiting");
        shutdown.signal();
        Ok(())
    }))
}

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread::spawn as spawn_thread;

use bytes::BytesMut;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::Future;
use futures_util::{stream::StreamExt, SinkExt};
use http::Uri;
use tokio::net::TcpStream;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{spawn, JoinHandle};
use tokio_websockets::{ClientBuilder, Error as WSError, MaybeTlsStream, Message, WebsocketStream};

use prism_schema::{
    pubsub::{client_greeting, client_message, server_greeting, server_message},
    Beam, ClientRequest, RequestType, ResponseType, ServerResponse,
};

fn write_to_message(message: Builder<HeapAllocator>) -> Message {
    let mut buffer: Vec<u8> = Vec::new();
    capnp::serialize::write_message(&mut buffer, &message).expect("Couldn't allocate memory"); // BUG potential
    let bytes = BytesMut::from(&buffer[..]);
    Message::binary(bytes)
}

fn sub_message(id: u64, beam: Beam, index: Option<u64>) -> Message {
    let rtype = RequestType::Subscribe(beam, index);
    let msg = ClientRequest { id, rtype };
    let string = serde_json::to_string(&msg).unwrap();

    Message::text(string)
}

fn unsub_message(id: u64, beam: Beam) -> Message {
    let rtype = RequestType::Unsubscribe(beam);
    let msg = ClientRequest { id, rtype };
    let string = serde_json::to_string(&msg).unwrap();
    Message::text(string)
}

fn emit_message(beam: String, payload: Vec<u8>) -> Message {
    let mut message = Builder::new(HeapAllocator::new());
    let mut client_msg = message.init_root::<client_message::Builder>();
    let mut emit = client_msg.reborrow().init_emission();
    emit.reborrow().init_beam(beam.len() as u32).push_str(&beam);
    emit.set_payload(&payload);

    write_to_message(message)
}

fn add_beam_message(id: u64, beam: Beam) -> Message {
    let rtype = RequestType::AddBeam(beam);
    let msg = ClientRequest { id, rtype };
    Message::text(serde_json::to_string(&msg).unwrap())
}

fn transmissions_message(id: u64) -> Message {
    let rtype = RequestType::ListBeams;
    let msg = ClientRequest { id, rtype };
    let string = serde_json::to_string(&msg).unwrap();

    Message::text(string)
}

fn greeting() -> Message {
    let mut message = Builder::new(HeapAllocator::new());
    let mut client_msg = message.init_root::<client_greeting::Builder>();
    client_msg.set_id(0);

    write_to_message(message)
}

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed,
    UnexpectedMessage,
    ErrorResult(String),
    Disconnected,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionFailed => write!(f, "ClientError::ConnectionFailed"),
            Self::UnexpectedMessage => write!(f, "ClientError::UnexpectedMessage"),
            Self::ErrorResult(s) => write!(f, "ClientError::ErrorResult({})", s),
            Self::Disconnected => write!(f, "ClientError::Disconnected"),
        }
    }
}

pub struct Photon {
    pub index: u64,
    pub time: i64,
    pub payload: Vec<u8>,
}

pub struct Wavelet {
    pub beam: Beam,
    pub photons: Vec<Photon>,
}

type Sink = SplitSink<WebsocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Stream = SplitStream<WebsocketStream<MaybeTlsStream<TcpStream>>>;

pub async fn connect(uri: Uri) -> Result<(u64, Sink, Stream), ClientError> {
    let (client, _) = ClientBuilder::from_uri(uri)
        .connect()
        .await
        .map_err(|_| ClientError::ConnectionFailed)?;
    let (mut write, mut read) = client.split();
    write
        .send(greeting())
        .await
        .map_err(|_| ClientError::ConnectionFailed)?;

    // Handle server messages and client ID
    let client_id = if let Some(message) = read.next().await {
        let data = message.unwrap().into_payload();
        let reader = serialize::read_message(data.as_ref(), ReaderOptions::default())
            .map_err(|_| ClientError::UnexpectedMessage)?;
        let greeting = reader
            .get_root::<server_greeting::Reader>()
            .map_err(|_| ClientError::UnexpectedMessage)?;

        // Extract client ID from server_message
        greeting.get_id()
    } else {
        return Err(ClientError::UnexpectedMessage);
    };
    tracing::info!("AsyncClient: {:?}", client_id);
    Ok((client_id, write, read))
}

fn inner_read_loop<F, G>(
    message: Message,
    response_fn: &F,
    message_fn: &mut G,
) -> Result<(), WSError>
where
    F: Fn(u64, ResponseType) -> Result<(), SendError<(u64, ResponseType)>>,
    G: Fn(Wavelet) -> Result<(), SendError<Wavelet>>,
{
    let mut wavelets = vec![];

    if message.is_binary() {
        let data = message.into_payload();
        let reader = serialize::read_message(data.as_ref(), ReaderOptions::default()).unwrap();
        let server_message = reader.get_root::<server_message::Reader>().unwrap();
        let beams = server_message.get_beams().unwrap();
        for beam in beams {
            let beam_name = beam.get_name().unwrap().to_string().unwrap();
            tracing::trace!("Beam: {}", beam_name);
            let photons = beam.get_photons().unwrap();
            let mut wav_vec = vec![];
            for photon in photons {
                let index = photon.get_index();
                let time = photon.get_time();
                let payload = photon.get_payload().unwrap();
                tracing::trace!("Index: {}, Time: {}, Data: {:?}", index, time, payload);
                wav_vec.push(Photon {
                    index,
                    time,
                    payload: payload.to_vec(),
                });
            }
            wavelets.push(Wavelet {
                beam: beam_name,
                photons: wav_vec,
            });
        }
    } else if message.is_text() {
        let data = message.as_text().unwrap();
        let ServerResponse { id, rtype } = serde_json::from_str(data).unwrap();
        if response_fn(id, rtype).is_err() {
            return Err(WSError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "response_fn",
            )));
        }
    }
    for wavelet in wavelets {
        if message_fn(wavelet).is_err() {
            return Err(WSError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message_fn",
            )));
        }
    }
    Ok(())
}

pub async fn read_loop<F, G>(
    mut read: Stream,
    response_fn: F,
    mut message_fn: G,
) -> Result<(), WSError>
where
    F: Fn(u64, ResponseType) -> Result<(), SendError<(u64, ResponseType)>>,
    G: Fn(Wavelet) -> Result<(), SendError<Wavelet>>,
{
    while let Some(message) = read.next().await {
        let message = message?;
        if inner_read_loop(message, &response_fn, &mut message_fn).is_err() {
            break;
        }
    }
    tracing::info!("Exit read thread");
    Ok::<_, WSError>(())
}

pub fn beam_result(
    rtype: ResponseType,
    all_beams: &mut HashSet<Beam>,
) -> Result<Vec<Beam>, ClientError> {
    match rtype {
        ResponseType::Beams(beams) => {
            all_beams.extend(beams);
            Ok(all_beams.iter().cloned().collect())
        }
        ResponseType::Ack => Err(ClientError::UnexpectedMessage),
        ResponseType::Error(err) => Err(ClientError::ErrorResult(err)),
    }
}

pub fn ack_result(rtype: ResponseType) -> Result<(), ClientError> {
    match rtype {
        ResponseType::Beams(_) => Err(ClientError::UnexpectedMessage),
        ResponseType::Ack => Ok(()),
        ResponseType::Error(err) => Err(ClientError::ErrorResult(err)),
    }
}

pub struct Client {
    msg_id: u64,
    message_into: mpsc::UnboundedSender<(Option<u64>, Message)>,
    response_from: std::sync::mpsc::Receiver<ResponseType>,
    beams: HashSet<Beam>,
}

impl Client {
    pub fn connect<F>(uri: Uri, wavelet_fn: F) -> Self
    where
        F: Fn(Wavelet) -> Result<(), Wavelet> + Send + Sync + 'static,
    {
        let (message_into, mut message_from) = mpsc::unbounded_channel();
        let (response_into, response_from) = std::sync::mpsc::channel();

        spawn_thread(move || {
            let rt = RuntimeBuilder::new_current_thread()
                .enable_io()
                .build()
                .unwrap();
            rt.block_on(async {
                // TODO: Reconnect
                let (_client_id, mut write, read) = match connect(uri).await {
                    Ok(result) => result,
                    Err(err) => {
                        eprintln!("Connection failed: {:?}", err);
                        return;
                    }
                };
                let responses: Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseType>>>> =
                    Arc::new(Mutex::new(HashMap::new()));
                let read_responses = responses.clone();

                spawn(async move {
                    read_loop(
                        read,
                        move |id, rtype| {
                            if let Some(tx) = read_responses.lock().unwrap().remove(&id) {
                                tx.send(rtype).ok();
                            };
                            Ok(())
                        },
                        |wavelet| wavelet_fn(wavelet).map_err(SendError),
                    )
                    .await
                });

                while let Some((id, msg)) = message_from.recv().await {
                    let (tx, rx) = oneshot::channel();

                    match id {
                        Some(id) => {
                            responses.lock().unwrap().insert(id, tx);
                            if write.send(msg).await.is_err() {
                                break;
                            }
                            let response = rx.await;
                            if let Ok(response) = response {
                                if response_into.send(response).is_err() {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                        None => {
                            if write.send(msg).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        });
        Self {
            msg_id: 0,
            message_into,
            response_from,
            beams: HashSet::new(),
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.msg_id;
        self.msg_id += 1;
        id
    }

    pub fn add_beam<B: Into<Beam>>(&mut self, beam: B) -> Result<(), ClientError> {
        let id = self.next_id();
        let msg = add_beam_message(id, beam.into());
        if self.message_into.send((Some(id), msg)).is_err() {
            return Err(ClientError::Disconnected);
        }
        match self.response_from.recv() {
            Ok(response) => ack_result(response),
            Err(_) => Err(ClientError::Disconnected),
        }
    }

    pub fn transmissions(&mut self) -> Result<Vec<Beam>, ClientError> {
        let id = self.next_id();
        let msg = transmissions_message(id);
        if self.message_into.send((Some(id), msg)).is_err() {
            return Err(ClientError::Disconnected);
        }
        match self.response_from.recv() {
            Ok(response) => beam_result(response, &mut self.beams),
            Err(_) => Err(ClientError::Disconnected),
        }
    }

    pub fn subscribe<B: Into<Beam>>(
        &mut self,
        beam: B,
        index: Option<u64>,
    ) -> Result<(), ClientError> {
        let id = self.next_id();
        let msg = sub_message(id, beam.into(), index);
        if self.message_into.send((Some(id), msg)).is_err() {
            return Err(ClientError::Disconnected);
        }
        match self.response_from.recv() {
            Ok(response) => ack_result(response),
            Err(_) => Err(ClientError::Disconnected),
        }
    }

    pub fn unsubscribe<B: Into<Beam>>(&mut self, beam: B) -> Result<(), ClientError> {
        let id = self.next_id();
        let msg = unsub_message(id, beam.into());
        if self.message_into.send((Some(id), msg)).is_err() {
            return Err(ClientError::Disconnected);
        }
        match self.response_from.recv() {
            Ok(response) => ack_result(response),
            Err(_) => Err(ClientError::Disconnected),
        }
    }

    pub fn emit<B: Into<Beam>>(&mut self, beam: B, data: Vec<u8>) -> Result<(), ClientError> {
        let msg = emit_message(beam.into(), data);
        if self.message_into.send((None, msg)).is_err() {
            return Err(ClientError::Disconnected);
        }
        Ok(())
    }
}

pub struct AsyncClient {
    client_id: u64, // TODO: reconnect
    cmd_id: u64,
    write: Sink,
    beams: HashSet<Beam>,
    read_task: JoinHandle<Result<(), tokio_websockets::Error>>,
    wavelet_task: JoinHandle<()>,
    responses: Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseType>>>>,
}

impl Drop for AsyncClient {
    fn drop(&mut self) {
        self.read_task.abort();
        self.wavelet_task.abort();
    }
}

impl AsyncClient {
    pub async fn connect<F, Fut>(uri: Uri, wavelet_fn: F) -> Result<Self, ClientError>
    where
        F: Fn(Wavelet) -> Fut + Sync + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let (client_id, write, read) = connect(uri).await?;
        let (tx_messages, mut rx_messages) = mpsc::unbounded_channel();
        let responses: Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseType>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let read_responses = responses.clone();
        let read_task = spawn(async move {
            read_loop(
                read,
                |id: u64, rtype: ResponseType| {
                    let mut lock = match read_responses.lock() {
                        Ok(lock) => lock,
                        Err(_) => return Err(SendError((id, rtype))),
                    };
                    if let Some(tx) = lock.remove(&id) {
                        tx.send(rtype).ok();
                    }
                    Ok(())
                },
                |wavelet| tx_messages.send(wavelet),
            )
            .await
        });
        let wavelet_task = spawn(async move {
            while let Some(msg) = rx_messages.recv().await {
                wavelet_fn(msg).await
            }
        });

        let mut client = Self {
            client_id,
            cmd_id: 0,
            write,
            beams: HashSet::new(),
            read_task,
            wavelet_task,
            responses,
        };
        client.transmissions().await?;
        Ok(client)
    }

    fn next_message(&mut self) -> (u64, oneshot::Receiver<ResponseType>) {
        let (tx, rx) = oneshot::channel();
        let id = self.cmd_id;
        self.cmd_id += 1;
        self.responses.lock().unwrap().insert(id, tx);
        (id, rx)
    }

    async fn send_msg<F>(&mut self, msg_fn: F) -> Result<ResponseType, ClientError>
    where
        F: FnOnce(u64) -> Message,
    {
        let (id, rx) = self.next_message();
        self.write
            .send(msg_fn(id))
            .await
            .map_err(|_| ClientError::Disconnected)?;
        rx.await.map_err(|_| ClientError::Disconnected)
    }

    pub async fn add_beam<B: Into<Beam>>(&mut self, beam: B) -> Result<(), ClientError> {
        let response = self
            .send_msg(|id| add_beam_message(id, beam.into()))
            .await?;
        ack_result(response)
    }

    pub async fn transmissions(&mut self) -> Result<Vec<Beam>, ClientError> {
        let response = self.send_msg(transmissions_message).await?;
        beam_result(response, &mut self.beams)
    }

    pub async fn subscribe<B: Into<Beam>>(
        &mut self,
        beam: B,
        index: Option<u64>,
    ) -> Result<(), ClientError> {
        let response = self
            .send_msg(|id| sub_message(id, beam.into(), index))
            .await?;
        ack_result(response)
    }

    pub async fn unsubscribe<B: Into<Beam>>(&mut self, beam: B) -> Result<(), ClientError> {
        let response = self.send_msg(|id| unsub_message(id, beam.into())).await?;
        ack_result(response)
    }

    pub async fn emit<B: Into<Beam>>(&mut self, beam: B, data: Vec<u8>) -> Result<(), ClientError> {
        self.write
            .send(emit_message(beam.into(), data))
            .await
            .map_err(|_| ClientError::Disconnected)
    }
}

#[cfg(test)]
mod tests {}

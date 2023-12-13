use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use capnp::message::{Builder, ReaderOptions, HeapAllocator};
use capnp::serialize;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, stream::StreamExt};
use http::Uri;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, mpsc};
use tokio::task::{spawn, JoinHandle};
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message, WebsocketStream, Error as WSError};


use prism_schema::{
    Beam,
    ClientRequest,
    RequestType,
    ServerResponse,
    ResponseType,
    pubsub::{
        client_greeting,
        server_greeting,
        client_message,
        server_message
    }
};


fn write_to_message(message: Builder<HeapAllocator>) -> Message
{
    let mut buffer: Vec<u8> = Vec::new();
    capnp::serialize::write_message(&mut buffer, &message)
        .expect("Couldn't allocate memory"); // BUG potential
    let bytes = BytesMut::from(&buffer[..]);
    Message::binary(bytes)
}

fn sub_message(id: u64, beam: Beam, index: Option<u64>) -> Message {
    let rtype = RequestType::Subscribe(beam, index);
    let msg = ClientRequest{id, rtype};
    let string = serde_json::to_string(&msg).unwrap();

    Message::text(string)
}

fn unsub_message(id: u64, beam: Beam) -> Message {
    let rtype = RequestType::Unsubscribe(beam);
    let msg = ClientRequest{id, rtype};
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
    let msg = ClientRequest{id, rtype};
    Message::text(serde_json::to_string(&msg).unwrap())
}

fn transmissions_message(id: u64) -> Message {
    let rtype = RequestType::ListBeams;
    let msg = ClientRequest{id, rtype};
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
    Disconnected
}


pub struct Photon {
    pub index: u64,
    pub time: i64,
    pub payload: Vec<u8>
}


pub struct Wavelet {
    pub beam: Beam,
    pub photons: Vec<Photon>
}


const CLIENT_MESSAGE_BUFFER: usize = 1024;


pub struct Client {
    client_id: u64,  // TODO: reconnect
    cmd_id: u64,
    write: SplitSink<WebsocketStream<MaybeTlsStream<TcpStream>>, Message>,
    beams: HashSet<Beam>,
    read_task: JoinHandle<Result<(), WSError>>,
    messages: Option<mpsc::Receiver<Wavelet>>,
    responses: Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseType>>>>,
}


impl Drop for Client {
    fn drop(&mut self) {
        self.read_task.abort();
    }
}


impl Client {
    pub async fn connect(uri: Uri) -> Result<Self, ClientError> {
        let (client, _) = ClientBuilder::from_uri(uri).connect().await
            .map_err(|_| ClientError::ConnectionFailed)?;
        let (mut write, mut read) = client.split();
        write.send(greeting()).await.map_err(|_| ClientError::ConnectionFailed)?;

        // Handle server messages and client ID
        let client_id = if let Some(message) = read.next().await {
            let data = message.unwrap().into_payload();
            let reader = serialize::read_message(
                data.as_ref(),
                ReaderOptions::default()
            ).map_err(|_|
                ClientError::UnexpectedMessage)?;
            let greeting = reader.get_root::<server_greeting::Reader>()
                .map_err(|_| ClientError::UnexpectedMessage)?;

            // Extract client ID from server_message
            greeting.get_id()
        } else {
            return Err(ClientError::UnexpectedMessage);
        };
        tracing::info!("Client: {:?}", client_id);
        let (tx_messages, rx_messages) = mpsc::channel(CLIENT_MESSAGE_BUFFER);
        let responses: Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseType>>>> = Arc::new(Mutex::new(HashMap::new()));
        let read_responses = responses.clone();

        let read_task = spawn(async move {
            while let Some(message) = read.next().await {
                let message = message?;
                let mut wavelets = vec![];

                if message.is_binary() {
                    let data = message.into_payload();
                    let reader = serialize::read_message(
                        data.as_ref(),
                        ReaderOptions::default()
                    ).unwrap();
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
                            wav_vec.push(Photon{index, time, payload: payload.to_vec()});
                        }
                        wavelets.push(Wavelet{beam: beam_name, photons: wav_vec});
                    }
                } else if message.is_text() {
                    let data = message.as_text().unwrap();
                    let msg: ServerResponse = serde_json::from_str(data).unwrap();
                    tracing::trace!("Server response: {:?}", msg);

                    if let Some(tx) = read_responses.lock().unwrap().remove(&msg.id) {
                        tx.send(msg.rtype).ok();
                    }
                }
                for wavelet in wavelets {
                    tx_messages.send(wavelet).await.unwrap();
                }
            }
            tracing::info!("Exit read thread");
            Ok::<_, WSError>(())
        });

        let mut client = Self{
            client_id,
            cmd_id: 0,
            write,
            beams: HashSet::new(),
            messages: Some(rx_messages),
            read_task,
            responses
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

    pub async fn add_beam<B: Into<Beam>>(&mut self, beam: B) -> Result<(), ClientError> {
        let (id, rx) = self.next_message();
        self.write.send(add_beam_message(id, beam.into())).await
            .map_err(|_| ClientError::Disconnected)?;
        let response = rx.await;

        match response {
            Ok(ResponseType::Beams(_)) => {
                Err(ClientError::UnexpectedMessage)
            }
            Ok(ResponseType::Ack) => {
                Ok(())
            }
            Ok(ResponseType::Error(err)) => {
                Err(ClientError::ErrorResult(err))
            }
            Err(_) => {
                Err(ClientError::Disconnected)
            }
        }
    }

    pub async fn transmissions(&mut self) -> Result<Vec<Beam>, ClientError> {
        let (id, rx) = self.next_message();
        self.write.send(transmissions_message(id)).await
            .map_err(|_| ClientError::Disconnected)?;

        let response = rx.await;
        match response {
            Ok(ResponseType::Beams(beams)) => {
                self.beams.extend(beams.iter().cloned());
                Ok(beams)
            }
            Ok(ResponseType::Ack) => {
                Err(ClientError::UnexpectedMessage)
            }
            Ok(ResponseType::Error(err)) => {
                Err(ClientError::ErrorResult(err))
            }
            Err(_) => {
                Err(ClientError::Disconnected)
            }
        }
    }

    pub async fn subscribe<B: Into<Beam>>(&mut self, beam: B, index: Option<u64>) -> Result<(), ClientError> {
        let (id, rx) = self.next_message();
        self.write.send(sub_message(id, beam.into(), index)).await
            .map_err(|_| ClientError::Disconnected)?;
        let response = rx.await;
        match response {
            Ok(ResponseType::Beams(_)) => {
                Err(ClientError::UnexpectedMessage)
            }
            Ok(ResponseType::Ack) => {
                Ok(())
            }
            Ok(ResponseType::Error(err)) => {
                Err(ClientError::ErrorResult(err))
            }
            Err(_) => {
                Err(ClientError::Disconnected)
            }
        }
    }

    pub async fn unsubscribe<B: Into<Beam>>(&mut self, beam: B) -> Result<(), ClientError> {
        let (id, rx) = self.next_message();
        self.write.send(unsub_message(id, beam.into())).await
            .map_err(|_| ClientError::Disconnected)?;
        let response = rx.await;
        match response {
            Ok(ResponseType::Beams(_)) => {
                Err(ClientError::UnexpectedMessage)
            }
            Ok(ResponseType::Ack) => {
                Ok(())
            }
            Ok(ResponseType::Error(err)) => {
                Err(ClientError::ErrorResult(err))
            }
            Err(_) => {
                Err(ClientError::Disconnected)
            }
        }
    }

    pub async fn emit<B: Into<Beam>>(&mut self, beam: B, data: Vec<u8>) -> Result<(), ClientError> {
        self.write.send(emit_message(beam.into(), data)).await.map_err(|_| ClientError::Disconnected)
    }

    pub fn get_message_rx(&mut self) -> Option<mpsc::Receiver<Wavelet>> {
        self.messages.take()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}

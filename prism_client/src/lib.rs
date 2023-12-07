use std::io::{Error, ErrorKind, Result};
use std::time::Duration;

use http::Uri;
use bytes::BytesMut;
use capnp::message::{Builder, ReaderOptions, HeapAllocator};
use capnp::serialize;
use futures_util::{SinkExt, stream::StreamExt};
use tokio::task::spawn;
use tokio::time::timeout;
use tokio_websockets::{ClientBuilder, Message};


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


fn capture_message(id: u64, beam: Beam, index: Option<u64>) -> Message {
    let rtype = RequestType::Subscribe(beam, index);
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

pub async fn run_client(url: &str) -> Result<()> {
    // 1. Establish WebSocket connection
    let uri = url.parse::<Uri>().unwrap();
    let (client, _) = ClientBuilder::from_uri(uri).connect().await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    let (mut write, mut read) = client.split();

    // 2. Send greeting
    write.send(greeting()).await.expect("First send failed");

    // 3. Handle server messages and client ID
    let client_id = if let Some(message) = read.next().await {
        let data = message.unwrap().into_payload();
        let reader = serialize::read_message(
            data.as_ref(),
            ReaderOptions::default()
        ).map_err(|e|
            Error::new(ErrorKind::Other, e.to_string()))?;
        let greeting = reader.get_root::<server_greeting::Reader>()
            .map_err(|e|
                Error::new(ErrorKind::Other, e.to_string()))?;

        // Extract client ID from server_message
        greeting.get_id()
    } else {
        return Err(Error::new(ErrorKind::Other, "Expected message"));
    };
    tracing::info!("Client: {:?}", client_id);

    let data: Vec<u8> = vec![0, 1, 2, 3];
    write.send(emit_message(String::from("beam1"), data)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    write.send(transmissions_message(client_id)).await.unwrap();

    let beams = {
        let message = read.next().await.expect("Server closed channel");
        let message = message.expect("Invalid message");
        if message.is_text() {
            let msg: ServerResponse = serde_json::from_str(message.as_text().unwrap())
                .expect("Invalid json");
            match msg.rtype {
                ResponseType::Beams(list) => list,
                _ => panic!("Unexpected")
            }
        } else {
            panic!("Only expected text response, not binary");
        }
    };

    for beam in beams {
        write.send(capture_message(client_id, beam, None)).await.unwrap();
    }

    let task = spawn(async move {
        while let Some(message) = read.next().await {
            let message = message?;

            if message.is_binary() {
                let data = message.into_payload();
                let reader = serialize::read_message(
                    data.as_ref(),
                    ReaderOptions::default()
                ).unwrap();
                let server_message = reader.get_root::<server_message::Reader>().unwrap();
                // Extract client ID from server_message
                let beams = server_message.get_beams().unwrap();
                for beam in beams {
                    tracing::info!("Beam: {}", beam.get_name().unwrap().to_string().unwrap());
                    let photons = beam.get_photons().unwrap();
                    for photon in photons {
                        tracing::info!("Index: {}, Time: {}, Data: {:?}",
                                       photon.get_index(),
                                       photon.get_time(),
                                       photon.get_payload());
                    }
                }
            } else {
                let data = message.as_text().unwrap();
                let msg: ServerResponse = serde_json::from_str(data).unwrap();
                println!("Server response: {:?}", msg);
            }
        }
        tracing::info!("Exit read thread");
        Ok::<_, tokio_websockets::Error>(())
    });

    loop {
        if task.is_finished() {
            break;
        }
        let data: Vec<u8> = vec![0, 1, 2, 3];
        let timeout_result = timeout(Duration::from_secs(3), write.send(emit_message(String::from("beam1"), data))).await;
        match timeout_result {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => break,
            Err(_) => {
                tracing::error!("Emit thread is stalled");
                break;
            }
        };
    }
    tracing::info!("Exit");
    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;
}

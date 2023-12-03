use std::io::{Error, ErrorKind, Result};
use http::Uri;
use bytes::BytesMut;
use capnp::message::{Builder, ReaderOptions, HeapAllocator};
use capnp::serialize;
use futures_util::{SinkExt, stream::StreamExt};
use tokio::task::spawn;
use tokio_websockets::{ClientBuilder, Message};

use prism_schema::pubsub::{client_message, server_message};


fn write_to_message(message: Builder<HeapAllocator>) -> Message
{
    let mut buffer: Vec<u8> = Vec::new();
    capnp::serialize::write_message(&mut buffer, &message)
        .expect("Couldn't allocate memory"); // BUG potential
    let bytes = BytesMut::from(&buffer[..]);
    Message::binary(bytes)
}


struct Capture {
    beam: String,
    index: Option<u64>
}

fn capture_message(id: u64, caps: Vec<Capture>) -> Message {
    let mut message = Builder::new(HeapAllocator::new());
    let mut client_msg = message.init_root::<client_message::Builder>();
    client_msg.set_id(id);
    let mut cmd = client_msg.reborrow().init_command();
    let mut msg_caps = cmd.reborrow().init_capture(caps.len() as u32);
    for (i, cap) in caps.into_iter().enumerate() {
        let mut set_cap = msg_caps.reborrow().get(i as u32);
        set_cap.reborrow().init_beam(cap.beam.len() as u32).push_str(&cap.beam);
        set_cap.set_index(if let Some(index) = cap.index { index as i64 } else { -1 });
    }
    write_to_message(message)
}

fn emit_message(id: u64, beam: String, payload: Vec<u8>) -> Message {
    let mut message = Builder::new(HeapAllocator::new());
    let mut client_msg = message.init_root::<client_message::Builder>();
    client_msg.set_id(id);
    let cmd = client_msg.reborrow().init_command();
    let mut emit = cmd.init_emit();
    emit.reborrow().init_beam(beam.len() as u32).push_str(&beam);
    emit.set_payload(&payload);

    write_to_message(message)
}

fn transmissions_message(id: u64) -> Message {
    let mut message = Builder::new(HeapAllocator::new());
    let mut client_msg = message.init_root::<client_message::Builder>();
    client_msg.set_id(id);
    let mut cmd = client_msg.reborrow().init_command();
    cmd.set_transmissions(());
    write_to_message(message)
}

pub async fn run_client(url: &str) -> Result<()> {
    // 1. Establish WebSocket connection
    let uri = url.parse::<Uri>().unwrap();
    let (client, _) = ClientBuilder::from_uri(uri).connect().await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    let (mut write, mut read) = client.split();

    // 2. Handle server messages and client ID
    let client_id = if let Some(message) = read.next().await {
        let data = message.unwrap().into_payload();
        let reader = serialize::read_message(
            data.as_ref(),
            ReaderOptions::default()
        ).map_err(|e|
            Error::new(ErrorKind::Other, e.to_string()))?;
        let server_message = reader.get_root::<server_message::Reader>()
            .map_err(|e|
                Error::new(ErrorKind::Other, e.to_string()))?;

        // Extract client ID from server_message
        match server_message.get_message().which() {
            Ok(server_message::message::Id(id)) => id,
            _ => return Err(Error::new(ErrorKind::Other, "Expected Id"))
        }
    } else {
        return Err(Error::new(ErrorKind::Other, "Expected message"));
    };
    tracing::info!("Client: {:?}", client_id);

    let data: Vec<u8> = vec![0, 1, 2, 3];
    write.send(emit_message(client_id, String::from("beam1"), data)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    write.send(transmissions_message(client_id)).await.unwrap();

    let captures = if let Some(message) = read.next().await {
        let data = message.unwrap().into_payload();
        let reader = serialize::read_message(
            data.as_ref(),
            ReaderOptions::default()
        ).map_err(|e|
            Error::new(ErrorKind::Other, e.to_string()))?;
        let server_message = reader.get_root::<server_message::Reader>()
            .map_err(|e|
                Error::new(ErrorKind::Other, e.to_string()))?;

        // Extract client ID from server_message
        let list = match server_message.get_message().which() {
            Ok(server_message::message::Transmissions(list)) => list.unwrap(),
            _ => return Err(Error::new(ErrorKind::Other, "Expected transmissions"))
        };

        let mut captures = Vec::new();
        for i in 0..list.len() {
            let entry = list.get(i).unwrap();
            let entry_str = entry.to_string().unwrap();
            tracing::info!("Entry: {}", entry_str);
            let cap = Capture{beam: entry_str, index: Some(0)};
            captures.push(cap);
        }
        captures
    } else {
        return Err(Error::new(ErrorKind::Other, "Expected message"));
    };

    write.send(capture_message(client_id, captures)).await.unwrap();

    let task = spawn(async move {
        while let Some(message) = read.next().await {
            let data = message.unwrap().into_payload();
            let reader = serialize::read_message(
                data.as_ref(),
                ReaderOptions::default()
            ).unwrap();
            let server_message = reader.get_root::<server_message::Reader>().unwrap();

            // Extract client ID from server_message
            match server_message.get_message().which() {
                Ok(server_message::message::Beams(beams)) => {
                    for beam in beams.unwrap() {
                        tracing::info!("Beam: {}", beam.get_name().unwrap().to_string().unwrap());
                        let photons = beam.get_photons().unwrap();
                        for photon in photons {
                            tracing::info!("Index: {}, Time: {}, Data: {:?}",
                                           photon.get_index(),
                                           photon.get_time(),
                                           photon.get_payload());
                        }
                    }
                },
                _ => return Err(Error::new(ErrorKind::Other, "Expected transmissions"))
            }
        }
        tracing::info!("Exit read thread");
        Ok(())
    });

    loop {
        if task.is_finished() {
            break;
        }
        let data: Vec<u8> = vec![0, 1, 2, 3];
        if write.send(emit_message(client_id, String::from("beam1"), data)).await.is_err() {
            break;
        }
    }
    tracing::info!("Exit");
    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;
}

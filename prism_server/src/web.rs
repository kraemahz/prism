use futures_util::{SinkExt, stream::StreamExt};
use std::sync::Arc;
use rustls::{server::NoClientAuth, ServerConfig};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::{fs::File, io::BufReader};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;



pub async fn handle_connection(raw_stream: TcpStream, tls_acceptor: Arc<TlsAcceptor>) {
    let tls_stream = tls_acceptor.accept(raw_stream).await.expect("Error during the TLS handshake");
    let ws_stream = accept_async(tls_stream).await.expect("Error during WebSocket handshake");

    // Split the WebSocket stream into a sender and receiver
    let (mut write, mut read) = ws_stream.split();

    // Echo messages received back to the client
    while let Some(message) = read.next().await {
        let message = message.expect("Error reading message");
        write.send(message).await.expect("Error sending message");
    }
}

use std::fs::File;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Parser;
use prism_server::beam::{BeamServerHandle, BeamsTable};
use prism_server::web::web_clients;
use prism_server::util::{ShutdownSender, shutdown_channel};
use serde::Deserialize;
use tracing_subscriber::{prelude::*, EnvFilter};

fn setup_tracing() {
    let tracing_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_level(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_file(true);
    #[cfg(debug_assertions)]
    {
        let filter_layer = EnvFilter::new("prism_server=debug");
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(tracing_layer)
            .init();
    }
    #[cfg(not(debug_assertions))]
    {
        let filter_layer = EnvFilter::new("prism_server=info");
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(tracing_layer)
            .init();
    }

    tracing::info!("Prism started");
}

async fn signal(shutdown: ShutdownSender) {
    if tokio::signal::ctrl_c().await.is_err() {
        tracing::error!("Failed to attach to ctrl-c signal");
    }
    shutdown.signal();
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    conf: PathBuf,
}

#[derive(Deserialize)]
struct TlsConfig {
    cert_path: String,
    key_path: String,
}

#[derive(Deserialize)]
struct PrismConfig {
    storage_dir: PathBuf,
    host: String,
    tls: Option<TlsConfig>,
}

const PRISM_PORT: u16 = 5050;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    setup_tracing();
    let args = Args::parse();
    let conf_file = File::open(args.conf).expect("Could not open file");
    let conf: PrismConfig = serde_json::from_reader(conf_file).expect("Reading config failed");
    let tls = conf.tls;

    let mut table = BeamsTable::new();
    let server = BeamServerHandle::new(conf.storage_dir, &mut table)
        .await
        .expect("Could not start beam server");

    let host: Ipv4Addr = conf.host.parse().expect("Invalid IP address");
    let addr: SocketAddr = SocketAddr::from((host, PRISM_PORT));
    tracing::info!("Listening on {:?}", addr);

    let routes = web_clients(
        Arc::new(Mutex::new(table)),
        server,
    );

    let (shutdown_tx, shutdown_rx) = shutdown_channel();
    tokio::spawn(signal(shutdown_tx));
    tokio::spawn(async move {
        if let Some(tls) = tls {
            warp::serve(routes)
                .tls()
                .cert_path(tls.cert_path.as_str())
                .key_path(tls.key_path.as_str())
                .run(addr)
                .await;
        } else {
            warp::serve(routes).run(addr).await;
        }
    });
    shutdown_rx.wait().await;

    Ok(())
}

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing_subscriber::{prelude::*, EnvFilter};

use prism_server::beam::{BeamServerHandle, BeamsTable};
use prism_server::util::{shutdown_channel, ShutdownSender};
use prism_server::web::run_web_server;

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
        let console_layer = console_subscriber::spawn();
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(console_layer)
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

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    setup_tracing();
    let mut table = BeamsTable::new();
    let base_dir = PathBuf::from("temp");
    let (shutdown_tx, shutdown_rx) = shutdown_channel();

    let server = BeamServerHandle::new(base_dir, &mut table)
        .await
        .expect("Could not start beam server");
    tracing::info!("Listening on {}", "127.0.0.1:5050");
    let web_task = run_web_server(
        "127.0.0.1:5050",
        Arc::new(Mutex::new(table)),
        server,
        shutdown_tx.clone(),
    )
    .await
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    let int_task = tokio::task::spawn(signal(shutdown_tx));

    // Await shutdown signal
    shutdown_rx.wait().await;
    tracing::info!("Shutting down");

    web_task.abort();
    web_task.await.ok();
    int_task.abort();
    int_task.await.ok();

    Ok(())
}

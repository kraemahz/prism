use prism_server::web::run_web_server;
use prism_server::exchange::run_queues;
use prism_server::router::{Router, BeamsTable};
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use tracing_subscriber::prelude::*;

use prism_server::util::{shutdown_channel, ShutdownSender};


fn setup_tracing() {
    //let tracing_layer = tracing_subscriber::fmt::layer()
    //    .compact()
    //    .with_level(true)
    //    .with_thread_ids(true)
    //    .with_line_number(true)
    //    .with_file(true);
    let console_layer = console_subscriber::spawn();

    tracing_subscriber::registry()
        .with(console_layer)
    //    .with(tracing_layer)
        .init();

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

    let router = Arc::new(Mutex::new(Router::new()));
    let table = Arc::new(Mutex::new(BeamsTable::new()));
    let base_dir = PathBuf::from("temp");
    let (shutdown_tx, shutdown_rx) = shutdown_channel();

    let q_task = run_queues(base_dir, router.clone(), table.clone(), shutdown_tx.clone()).await
        .map_err(|e| std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string()
                ))?;
    let web_task = run_web_server("127.0.0.1:5050", router, table, shutdown_tx.clone()).await
        .map_err(|e| std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string()
                ))?;
    let int_task = tokio::task::spawn(signal(shutdown_tx));

    // Await shutdown signal
    shutdown_rx.wait().await;
    tracing::info!("Shutting down");

    q_task.abort();
    q_task.await.ok();
    web_task.abort();
    web_task.await.ok();
    int_task.abort();
    int_task.await.ok();

    Ok(())
}

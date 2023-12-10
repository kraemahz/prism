use prism_client::run_client;


fn setup_tracing() {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_level(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_file(true)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set up tracing");
}


#[tokio::main(flavor="multi_thread", worker_threads=4)]
async fn main() -> std::io::Result<()> {
    setup_tracing();
    run_client("ws://127.0.0.1:5050").await?;
    Ok(())
}

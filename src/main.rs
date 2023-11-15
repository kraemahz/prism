use prism::queue::create_durable_queue;
use std::path::Path;


#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let path = Path::new("temp/");
    let (_w, _r) = create_durable_queue(&path, "test").await?;
    Ok(())
}

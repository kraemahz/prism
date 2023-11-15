use criterion::{criterion_group, criterion_main, Criterion};
use prism::queue::{DurableQueueWriter, DurableQueueReader, create_durable_queue};
use tempfile::{tempdir, TempDir};
use tokio::runtime::Runtime;
use std::rc::Rc;
use std::cell::RefCell;


fn setup() -> TempDir { 
    tempdir().expect("Failed to create temporary directory")
}

const PAYLOAD: [u8; 256] = [0u8; 256];

async fn push_benchmark(writer: Rc<RefCell<DurableQueueWriter>>) {
    writer.borrow_mut().push(&PAYLOAD).await.expect("Failed to push to queue");
}

async fn read_benchmark(writer: Rc<RefCell<DurableQueueWriter>>, reader: Rc<RefCell<DurableQueueReader>>) {
    writer.borrow_mut().push(&PAYLOAD).await.expect("Failed to push to queue");
    reader.borrow_mut().read_next().await.expect("Faield to read message");
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create a runtime");

    c.bench_function("push_benchmark", |b| {
        let dir = setup();
        let base_dir = dir.path();
        let (writer, _) = rt.block_on(create_durable_queue(base_dir, "push_bench")).expect("Failed to create queue");
        let w = Rc::new(RefCell::new(writer));
        b.to_async(&rt).iter(move || push_benchmark(w.clone()));
    });

    c.bench_function("read_benchmark", |b| {
        let dir = setup();
        let base_dir = dir.path();
        let (writer, reader) = rt.block_on(create_durable_queue(base_dir, "read_bench")).expect("Failed to create queue");
        let w = Rc::new(RefCell::new(writer));
        let r = Rc::new(RefCell::new(reader));
        b.to_async(&rt).iter(move || read_benchmark(w.clone(), r.clone()));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

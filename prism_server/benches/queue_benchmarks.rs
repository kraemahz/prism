use criterion::{criterion_group, criterion_main, Criterion};
use prism_server::queue::{DurableQueueReader, DurableQueueWriter};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

fn setup() -> TempDir {
    tempdir().expect("Failed to create temporary directory")
}

const PAYLOAD: [u8; 256] = [0u8; 256];

async fn push_benchmark(writer: Rc<RefCell<DurableQueueWriter>>) {
    writer
        .borrow_mut()
        .push(&PAYLOAD)
        .await
        .expect("Failed to push to queue");
}

async fn read_benchmark(
    writer: Rc<RefCell<DurableQueueWriter>>,
    reader: Rc<RefCell<DurableQueueReader>>,
) {
    writer
        .borrow_mut()
        .push(&PAYLOAD)
        .await
        .expect("Failed to push to queue");
    reader
        .borrow_mut()
        .next()
        .await
        .expect("Faield to read message");
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create a runtime");

    c.bench_function("push_benchmark", |b| {
        let dir = setup();
        let base_dir = dir.path();

        let beam: Arc<str> = Arc::from("push_bench");
        let writer = rt
            .block_on(DurableQueueWriter::create_by_beam(
                base_dir,
                beam.clone(),
                0,
            ))
            .expect("writer");
        let w = Rc::new(RefCell::new(writer));
        b.to_async(&rt).iter(move || push_benchmark(w.clone()));
    });

    c.bench_function("read_benchmark", |b| {
        let dir = setup();
        let base_dir = dir.path();
        let beam: Arc<str> = Arc::from("read_bench");
        let (_queue_tx, queue_rx) = broadcast::channel(1);

        let writer = rt
            .block_on(DurableQueueWriter::create_by_beam(
                base_dir,
                beam.clone(),
                0,
            ))
            .expect("writer");

        let reader = rt
            .block_on(DurableQueueReader::new(
                beam,
                base_dir.to_path_buf(),
                queue_rx,
                0,
                0,
            ))
            .expect("reader");

        let w = Rc::new(RefCell::new(writer));
        let r = Rc::new(RefCell::new(reader));
        b.to_async(&rt)
            .iter(move || read_benchmark(w.clone(), r.clone()));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

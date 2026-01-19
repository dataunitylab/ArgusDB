use argusdb::log::{Log, Logger, Operation};
use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::tempdir;

fn logging_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("logging");
    group.sample_size(10);

    group.bench_function("write_log_entry", |b| {
        b.iter_custom(|iters| {
            let dir = tempdir().unwrap();
            let log_path = dir.path().join("test.log");
            let mut logger = Logger::new(&log_path, 1024 * 1024).unwrap(); // 1MB rotation threshold
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let op = Operation::Insert {
                    id: "test_doc_id".to_string(),
                    doc: serde_json::json!({"key": "value"}),
                };

                logger.log(op).unwrap();
            }

            start.elapsed()
        })
    });

    group.finish();
}

criterion_group!(benches, logging_benchmark);
criterion_main!(benches);

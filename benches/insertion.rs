use argusdb::db::DB;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::json;
use std::hint;
use tempfile::tempdir;

fn insertion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("insertion");
    let num_docs = 10_000;
    group.throughput(Throughput::Elements(num_docs as u64));

    group.bench_function(BenchmarkId::new("insert", num_docs), |b| {
        b.iter_custom(|iters| {
            let mut total_duration = std::time::Duration::new(0, 0);
            for _ in 0..iters {
                // Setup for each iteration: Create a new DB in a temp directory
                let dir = tempdir().unwrap();
                let mut db = DB::new(dir.path().to_str().unwrap(), num_docs + 1, 10);
                db.create_collection("test").unwrap();

                let start = std::time::Instant::now();

                // Benchmarked routine: Insert num_docs
                for i in 0..num_docs {
                    db.insert("test", hint::black_box(json!({ "a": i })))
                        .unwrap();
                }

                total_duration += start.elapsed();

                // Teardown for each iteration: `dir` (TempDir) is dropped here,
                // cleaning up the temporary database files.
            }
            total_duration
        })
    });

    group.finish();
}

criterion_group!(benches, insertion_benchmark);
criterion_main!(benches);

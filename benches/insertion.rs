use argusdb::db::DB;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::json;
use std::hint;
use tempfile::tempdir;

fn insertion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("insertion");

    let num_docs = 10_000;

    group.throughput(Throughput::Elements(num_docs as u64));

    for num_keys in [1, 10, 100].iter() {
        group.measurement_time(std::time::Duration::from_secs(*num_keys * 5));
        group.bench_function(BenchmarkId::new("insert", *num_keys), |b| {
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
                        let doc = {
                            let mut doc = serde_json::Map::new();

                            for j in 0..*num_keys {
                                doc.insert(format!("key{}", j), serde_json::Value::from(i));
                            }

                            serde_json::Value::Object(doc)
                        };

                        db.insert("test", hint::black_box(doc)).unwrap();
                    }

                    total_duration += start.elapsed();

                    // Teardown for each iteration: `dir` (TempDir) is dropped here,
                    // cleaning up the temporary database files.
                }

                total_duration
            })
        });
    }

    group.finish();
}

criterion_group!(benches, insertion_benchmark);
criterion_main!(benches);

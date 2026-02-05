use argusdb::bench_utils::{save_profile, start_profiling};
use argusdb::log::{Log, Logger, Operation};
use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::tempdir;

fn logging_benchmark(c: &mut Criterion) {
    let profile_path = std::env::var("ARGUS_PROFILE").ok().map(|p| {
        if p.is_empty() {
            "logging_profile.pb".to_string()
        } else {
            format!("logging_{}", p)
        }
    });

    let guard = start_profiling(&profile_path);

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
                    doc: serde_json::json!({"key": "value"}).into(),
                };

                logger.log(op).unwrap();
            }

            start.elapsed()
        })
    });

    group.finish();

    save_profile(guard, &profile_path);
}

criterion_group!(benches, logging_benchmark);
criterion_main!(benches);

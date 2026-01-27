use argusdb::db::DB;
use argusdb::query::{BinaryOperator, Expression, LogicalPlan, execute_plan};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::json;
use std::hint;
use tempfile::tempdir;

fn generate_doc_with_keys(num_keys: usize, i: usize) -> serde_json::Value {
    let mut doc = serde_json::Map::new();
    for j in 0..num_keys {
        doc.insert(format!("key{}", j), serde_json::Value::from(i));
    }
    serde_json::Value::Object(doc)
}

fn generate_query_plan(collection_name: &str, selectivity: f64, total_docs: usize) -> LogicalPlan {
    let scan_plan = LogicalPlan::Scan {
        collection: collection_name.to_string(),
    };

    if selectivity >= 1.0 {
        // 100% selectivity or more, no filter needed
        return scan_plan;
    }

    let filter_value = (total_docs as f64 * selectivity).round() as i64;

    let predicate = Expression::Binary {
        left: Box::new(Expression::FieldReference("value".to_string())),
        op: BinaryOperator::Lt,
        right: Box::new(Expression::Literal(json!(filter_value).into())),
    };

    LogicalPlan::Filter {
        input: Box::new(scan_plan),
        predicate,
    }
}

fn insertion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("insertion");
    group.sample_size(10);
    let max_docs = 10_000;

    for num_keys in [1usize, 10usize, 100usize].iter() {
        group.bench_function(BenchmarkId::new("insert", *num_keys), |b| {
            b.iter_custom(|iters| {
                let mut total_duration = std::time::Duration::new(0, 0);
                for _ in 0..iters {
                    // Setup for each iteration: Create a new DB in a temp directory
                    let dir = tempdir().unwrap();
                    let mut db =
                        DB::new(dir.path().to_str().unwrap(), max_docs + 1, 10, 1024, None);
                    db.create_collection("test").unwrap();

                    let start = std::time::Instant::now();

                    // Benchmarked routine: Insert documents
                    for i in 0..(max_docs / num_keys) {
                        let doc = generate_doc_with_keys(*num_keys, i);
                        db.insert("test", hint::black_box(doc.into())).unwrap();
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

fn query_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("query");
    group.sample_size(10);
    let num_docs = 10_000;
    let collection_name = "test";

    // Setup: Insert 10,000 documents with a 'value' field
    // This setup will be run once per sample for iter_custom (which is not what we want)
    // So we need to put the DB setup inside the iter_custom loop
    // But then I cannot use group.throughput

    // So for query benchmark, the setup has to be outside of the iter_custom entirely for the benchmark group
    // This implies that the DB must be created once and then queries run against it.
    // However, the original insertion benchmark creates a *new* DB for every iteration (not every sample).
    // This is to avoid cumulative effects (e.g. flushing to disk etc.)

    // Let's create a separate setup for queries that returns the DB
    let dir = tempdir().unwrap();
    let mut db = DB::new(dir.path().to_str().unwrap(), num_docs + 1, 10, 1024, None); // Don't flush
    db.create_collection(collection_name).unwrap();
    for i in 0..num_docs {
        db.insert(collection_name, json!({"value": i}).into())
            .unwrap();
    }
    let db_arc = std::sync::Arc::new(std::sync::Mutex::new(db));

    for selectivity in [1.0, 0.1, 0.01].iter() {
        let plan = generate_query_plan(collection_name, *selectivity, num_docs);
        group.throughput(Throughput::Elements(
            (num_docs as f64 * *selectivity).round() as u64,
        ));

        group.bench_function(BenchmarkId::new("query", selectivity), |b| {
            b.iter_custom(|iters| {
                let mut total_duration = std::time::Duration::new(0, 0);
                for _ in 0..iters {
                    // For each iteration, acquire the DB lock and execute the query
                    let db_lock = db_arc.lock().unwrap();

                    let start = std::time::Instant::now();

                    // Execute the query plan and iterate over all results
                    let mut iter = execute_plan(plan.clone(), &db_lock).unwrap();
                    while iter.next().is_some() {} // Consume all results

                    total_duration += start.elapsed();
                }
                total_duration
            })
        });
    }

    group.finish();
}

criterion_group!(benches, insertion_benchmark, query_benchmark);
criterion_main!(benches);

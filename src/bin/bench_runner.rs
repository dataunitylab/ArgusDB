use argusdb::db::DB;
use argusdb::parser;
use argusdb::query::{Statement, execute_plan};
use argusdb::serde_to_jsonb;
use clap::Parser;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::StdRng;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::sync::Mutex;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    concurrency: usize,

    #[arg(short, long, default_value_t = 5)]
    warmup: u64,

    #[arg(short, long, default_value_t = 30)]
    duration: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // 1. Setup DB
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_str().unwrap();
    println!("Setting up DB at {}", db_path);

    // Use reasonable defaults for bench
    let memtable_threshold = 1000;
    let jstable_threshold = 10;
    let index_threshold = 1024;

    let db = Arc::new(Mutex::new(DB::new(
        db_path,
        memtable_threshold,
        jstable_threshold,
        index_threshold,
        None, // No log rotation for bench? Or maybe yes.
    )));

    // 2. Load Data
    let data_dir = Path::new("workbook/data");
    if data_dir.exists() {
        for entry in fs::read_dir(data_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let stem = path.file_stem().unwrap().to_str().unwrap();
                let collection_name = stem.to_string();
                println!("Loading collection: {}", collection_name);

                {
                    let mut db_guard = db.lock().await;
                    db_guard.create_collection(&collection_name).unwrap();

                    let content = fs::read_to_string(&path).unwrap();
                    for line in content.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        let json_val: serde_json::Value = serde_json::from_str(line).unwrap();
                        let doc = serde_to_jsonb(json_val);
                        db_guard.insert(&collection_name, doc).unwrap();
                    }
                }
            }
        }
    } else {
        println!("workbook/data not found!");
        return;
    }

    // 3. Load Queries
    let queries_dir = Path::new("workbook/queries");
    let mut queries = Vec::new();
    if queries_dir.exists() {
        for entry in fs::read_dir(queries_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                let name = path.file_name().unwrap().to_str().unwrap().to_string();
                let sql = fs::read_to_string(&path).unwrap();
                // Adapt query: Remove "collection". prefix
                // Since we created collection with name "mycol" (from mycol.json),
                // and queries use "mycol"."field", we strip "mycol".
                // We do this generically for any collection name found in data loading?
                // For simplicity, let's assume we strip the collection name if it matches what we loaded.
                // Or just strip any "xxx". prefix? NO, that breaks "c0"."b1".
                // We need to strip specific collection aliases.
                // But here we can just strip "mycol". specifically or do it smarter.
                // Given the prompt "Files in the workbook directory may not be modified", adaptation is key.
                // Let's assume the query uses the collection name as table alias.
                // We'll replace "mycol"." with empty string.
                // Note: The SQL might contain FROM "mycol". We KEEP that.
                // We only want to remove "mycol"." from column references.
                // `sqlparser` creates FieldReference("mycol.b0"). ArgusDB scans "b0".
                // So adaptation is needed.

                // Adapt query: Remove "mycol". prefix and strip double quotes to ensure CompoundIdentifier
                let adapted_sql = sql.replace("\"mycol\".", "").replace("\"", "");

                queries.push((name, adapted_sql));
            }
        }
    }

    if queries.is_empty() {
        println!("No queries found!");
        return;
    }

    // Sort queries to be deterministic
    queries.sort_by(|a, b| a.0.cmp(&b.0));
    let queries = Arc::new(queries);

    println!("Starting warmup for {} seconds...", args.warmup);
    run_phase(
        args.concurrency,
        args.warmup,
        db.clone(),
        queries.clone(),
        false,
    )
    .await;

    println!("Starting measurement for {} seconds...", args.duration);
    let results = run_phase(
        args.concurrency,
        args.duration,
        db.clone(),
        queries.clone(),
        true,
    )
    .await;

    println!("Results:");
    for (name, (count, total_time)) in results {
        let avg = if count > 0 {
            total_time.as_secs_f64() / count as f64
        } else {
            0.0
        };
        println!("{}: {:.4}s ({} runs)", name, avg, count);
    }
}

async fn run_phase(
    concurrency: usize,
    duration_secs: u64,
    db: Arc<Mutex<DB>>,
    queries: Arc<Vec<(String, String)>>,
    record: bool,
) -> std::collections::BTreeMap<String, (usize, Duration)> {
    let start_time = Instant::now();
    let duration = Duration::from_secs(duration_secs);
    let mut handles = Vec::new();

    // Shared results: Mutex<BTreeMap<QueryName, (Count, TotalTime)>>
    let results: Arc<Mutex<std::collections::BTreeMap<String, (usize, Duration)>>> =
        Arc::new(Mutex::new(std::collections::BTreeMap::new()));

    for _ in 0..concurrency {
        let db = db.clone();
        let queries = queries.clone();
        let results = results.clone();

        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::from_os_rng();
            loop {
                if start_time.elapsed() >= duration {
                    break;
                }

                let (name, sql) = queries.choose(&mut rng).unwrap();

                // Execute
                let q_start = Instant::now();

                // Parse
                let stmt = match parser::parse(sql) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Error parsing {}: {}", name, e);
                        continue;
                    }
                };

                // Run
                match stmt {
                    Statement::Select(plan) => {
                        let db_guard = db.lock().await;
                        // execute_plan returns iterator. We must consume it.
                        if let Ok(iter) = execute_plan(plan, &db_guard) {
                            for _ in iter {} // Consume
                        } else {
                            eprintln!("Error executing {}", name);
                        }
                    }
                    _ => eprintln!("Unsupported statement in {}", name),
                }

                let q_duration = q_start.elapsed();

                if record {
                    let mut res = results.lock().await;
                    let entry = res.entry(name.clone()).or_insert((0, Duration::new(0, 0)));
                    entry.0 += 1;
                    entry.1 += q_duration;
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let res = results.lock().await;
    (*res).clone()
}

use argusdb::bench_utils::{Args, Query, load_queries, run_measurement};
use argusdb::db::DB;
use argusdb::parser;
use argusdb::query::{Statement, execute_plan};
use argusdb::serde_to_jsonb;
use clap::Parser;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Mutex;

async fn execute_query(db: Arc<Mutex<DB>>, query: Query) {
    // Parse
    let stmt = match parser::parse(&query.sql) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error parsing {}: {}", query.name, e);
            return;
        }
    };

    // Run
    match stmt {
        Statement::Select(plan) => {
            let db_guard = db.lock().await;
            if let Ok(iter) = execute_plan(plan, &db_guard) {
                for _ in iter {} // Consume
            } else {
                eprintln!("Error executing {}", query.name);
            }
        }
        _ => eprintln!("Unsupported statement in {}", query.name),
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // 1. Setup DB
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_str().unwrap();
    println!("Setting up DB at {}", db_path);

    let db = Arc::new(Mutex::new(DB::new(db_path, 1000, 10, 1024, None)));

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
    let queries = Arc::new(load_queries());
    if queries.is_empty() {
        println!("No queries found!");
        return;
    }

    println!("Starting warmup for {} seconds...", args.warmup);
    run_measurement(
        args.concurrency,
        args.warmup,
        db.clone(),
        queries.clone(),
        execute_query,
        false,
    )
    .await;

    println!("Starting measurement for {} seconds...", args.duration);
    let results = run_measurement(
        args.concurrency,
        args.duration,
        db.clone(),
        queries.clone(),
        execute_query,
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

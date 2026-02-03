#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use argusdb::bench_utils::{
    Args, Query, load_queries, run_measurement, save_profile, start_profiling,
};
use clap::Parser;
use rand::prelude::IndexedRandom;
use std::fs;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};

#[derive(Clone)]
struct Context {
    clients: Arc<Vec<Client>>,
}

struct ServerGuard {
    child: Child,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn execute_query(ctx: Context, query: Query) {
    let client = {
        let mut rng = rand::rng();
        ctx.clients.as_slice().choose(&mut rng).unwrap()
    };

    // Execute
    match client.simple_query(query.sql.as_str()).await {
        Ok(messages) => for _ in messages {},
        Err(e) => {
            // Ignore errors for "drop collection" if it doesn't exist etc,
            // but usually benchmark queries are valid.
            eprintln!("Error executing {}: {}", query.name, e);
        }
    }
}

async fn connect_with_retry(config: &str) -> Option<(Client, tokio::task::JoinHandle<()>)> {
    for _ in 0..50 {
        match tokio_postgres::connect(config, NoTls).await {
            Ok((client, connection)) => {
                let handle = tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("connection error: {}", e);
                    }
                });
                return Some((client, handle));
            }
            Err(_) => {
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    None
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // 1. Setup DB Directory
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_str().unwrap().to_string();
    println!("Setting up DB at {}", db_path);

    // 2. Start ArgusDB Server
    // We assume `argusdb` binary is available. We can use `cargo run` but that might recompile.
    // Better to invoke the binary directly if possible, or use cargo run.
    // Using cargo run ensures it runs the current code.
    let port = 5433;
    let host = "127.0.0.1";

    println!("Starting ArgusDB server on {}:{}", host, port);
    let mut command = Command::new("./target/release/argusdb");
    command.args(&[
        "--jstable-dir",
        &db_path,
        "--host",
        host,
        "--port",
        &port.to_string(),
        "--memtable-threshold",
        "1000",
        "--jstable-threshold",
        "10",
        "--index-threshold",
        "1024",
    ]);

    if args.no_log {
        command.arg("--no-log");
    }

    let child = command
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to start argusdb server");

    let _server_guard = ServerGuard { child };

    // Wait for server to be ready and create initial connection
    let conn_str = format!("host={} port={} user=postgres", host, port);
    let (init_client, _) = connect_with_retry(&conn_str)
        .await
        .expect("Failed to connect to ArgusDB server");

    // 3. Load Data
    let data_dir = Path::new("workbook/data");
    if data_dir.exists() {
        for entry in fs::read_dir(data_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let stem = path.file_stem().unwrap().to_str().unwrap();
                let collection_name = stem.to_string();
                println!("Loading collection: {}", collection_name);

                // Create Collection
                let create_sql = format!("CREATE COLLECTION {}", collection_name);
                init_client.simple_query(&create_sql).await.unwrap();

                let content = fs::read_to_string(&path).unwrap();
                for line in content.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    // Validate JSON locally just in case, but usually we just wrap it.
                    // The line is expected to be a valid JSON object.
                    // SQL: INSERT INTO col VALUES (`json`)
                    // We need to be careful if json contains backticks, but standard JSON doesn't.
                    let insert_sql = format!("INSERT INTO {} VALUES (`{}`)", collection_name, line);
                    if let Err(e) = init_client.simple_query(&insert_sql).await {
                        eprintln!("Failed to insert document: {}", e);
                    }
                }
            }
        }
    } else {
        println!("workbook/data not found!");
        return;
    }

    // 4. Establish connections for benchmark
    let mut clients = Vec::new();
    let mut _handles = Vec::new();

    // We already have init_client, we can reuse it?
    // But we need `args.concurrency` clients.
    // Let's create new ones.
    println!("Establishing {} connections...", args.concurrency);
    for _ in 0..args.concurrency {
        let (c, h) = connect_with_retry(&conn_str)
            .await
            .expect("Failed to connect");
        clients.push(c);
        _handles.push(h);
    }

    let context = Context {
        clients: Arc::new(clients),
    };

    // 5. Load Queries
    let queries = Arc::new(load_queries());
    if queries.is_empty() {
        println!("No queries found!");
        return;
    }

    println!("Starting warmup for {} seconds...", args.warmup);
    run_measurement(
        args.concurrency,
        args.warmup,
        context.clone(),
        queries.clone(),
        execute_query,
        false,
    )
    .await;

    println!("Starting measurement for {} seconds...", args.duration);
    #[cfg(feature = "profiling")]
    let guard = start_profiling(args.profile);
    #[cfg(not(feature = "profiling"))]
    let guard = start_profiling(false);

    let results = run_measurement(
        args.concurrency,
        args.duration,
        context.clone(),
        queries.clone(),
        execute_query,
        true,
    )
    .await;

    save_profile(guard);

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

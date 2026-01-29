use clap::Parser;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::StdRng;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value_t = 1)]
    pub concurrency: usize,

    #[arg(short, long, default_value_t = 5)]
    pub warmup: u64,

    #[arg(short, long, default_value_t = 30)]
    pub duration: u64,
}

#[derive(Clone)]
pub struct Query {
    pub name: String,
    pub sql: String,
}

pub fn load_queries() -> Vec<Query> {
    let queries_dir = Path::new("workbook/queries");
    let mut queries = Vec::new();
    if queries_dir.exists() {
        for entry in fs::read_dir(queries_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                let name = path.file_name().unwrap().to_str().unwrap().to_string();
                let sql = fs::read_to_string(&path).unwrap();
                let adapted_sql = sql.replace("\"mycol\".", "").replace("\"", "");
                queries.push(Query {
                    name,
                    sql: adapted_sql,
                });
            }
        }
    }
    queries.sort_by(|a, b| a.name.cmp(&b.name));
    queries
}

pub async fn run_measurement<C, F, Fut>(
    concurrency: usize,
    duration_secs: u64,
    context: C,
    queries: Arc<Vec<Query>>,
    execute_fn: F,
    record: bool,
) -> BTreeMap<String, (usize, Duration)>
where
    C: Clone + Send + Sync + 'static,
    F: Fn(C, Query) -> Fut + Send + Sync + 'static + Copy,
    Fut: std::future::Future<Output = ()> + Send,
{
    let start_time = Instant::now();
    let duration = Duration::from_secs(duration_secs);
    let mut handles = Vec::new();

    let results: Arc<Mutex<BTreeMap<String, (usize, Duration)>>> =
        Arc::new(Mutex::new(BTreeMap::new()));

    for _ in 0..concurrency {
        let ctx = context.clone();
        let queries = queries.clone();
        let results = results.clone();

        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::from_os_rng();
            loop {
                if start_time.elapsed() >= duration {
                    break;
                }

                let query = queries.choose(&mut rng).unwrap();
                let q_start = Instant::now();

                execute_fn(ctx.clone(), query.clone()).await;

                let q_duration = q_start.elapsed();

                if record {
                    let mut res = results.lock().await;
                    let entry = res
                        .entry(query.name.clone())
                        .or_insert((0, Duration::new(0, 0)));
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

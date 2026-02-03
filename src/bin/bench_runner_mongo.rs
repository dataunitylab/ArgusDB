#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "mongo")]
use argusdb::bench_utils::{
    Args, Query, load_queries, run_measurement, save_profile, start_profiling,
};
#[cfg(feature = "mongo")]
use argusdb::{
    jsonb_to_serde, parser,
    query::{BinaryOperator, Expression, LogicalOperator, LogicalPlan, ScalarFunction, Statement},
};
#[cfg(feature = "mongo")]
use bumpalo::Bump;
#[cfg(feature = "mongo")]
use clap::Parser;
#[cfg(feature = "mongo")]
use mongodb::{Client, bson::Bson, bson::Document, bson::doc, options::ClientOptions};
#[cfg(feature = "mongo")]
use std::fs;
#[cfg(feature = "mongo")]
use std::path::Path;
#[cfg(feature = "mongo")]
use std::sync::Arc;

#[cfg(not(feature = "mongo"))]
fn main() {
    println!("This binary requires the 'mongo' feature. Run with --features mongo");
}

#[cfg(feature = "mongo")]
#[tokio::main]
async fn main() {
    let args = Args::parse();

    let client_uri =
        std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
    println!("Connecting to MongoDB at {}", client_uri);

    let client_options = ClientOptions::parse(&client_uri)
        .await
        .expect("Failed to parse MongoDB URI");
    let client = Client::with_options(client_options).expect("Failed to create MongoDB client");
    let db = client.database("argus_bench");

    // Load Data
    let data_dir = Path::new("workbook/data");
    if data_dir.exists() {
        for entry in fs::read_dir(data_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let stem = path.file_stem().unwrap().to_str().unwrap();
                let collection_name = stem.to_string();
                println!("Loading collection: {}", collection_name);

                let collection = db.collection::<Document>(&collection_name);
                collection.drop().await.ok();

                let content = fs::read_to_string(&path).unwrap();
                let mut docs = Vec::new();
                for line in content.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let json_val: serde_json::Value = serde_json::from_str(line).unwrap();
                    let bson_val = json_to_bson(&json_val);
                    if let Bson::Document(doc) = bson_val {
                        docs.push(doc);
                    }
                }
                if !docs.is_empty() {
                    collection.insert_many(docs).await.unwrap();
                }
            }
        }
    } else {
        println!("workbook/data not found!");
        return;
    }

    let queries = Arc::new(load_queries());
    let ctx = Arc::new(db);

    if queries.is_empty() {
        println!("No queries found!");
        return;
    }

    println!("Starting warmup for {} seconds...", args.warmup);
    run_measurement(
        args.concurrency,
        args.warmup,
        ctx.clone(),
        queries.clone(),
        execute_mongo_query,
        false,
    )
    .await;

    println!("Starting measurement for {} seconds...", args.duration);
    let guard = start_profiling(&args.profile);

    let results = run_measurement(
        args.concurrency,
        args.duration,
        ctx.clone(),
        queries.clone(),
        execute_mongo_query,
        true,
    )
    .await;
    save_profile(guard, &args.profile);

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

#[cfg(feature = "mongo")]
async fn execute_mongo_query(db: Arc<mongodb::Database>, query: Query) {
    let arena = Bump::new();
    let stmt = match parser::parse(&query.sql, &arena) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error parsing {}: {}", query.name, e);
            return;
        }
    };

    match stmt {
        Statement::Select(plan) => {
            let mut current = &plan;
            let mut limit = None;
            let mut offset = None;
            let mut project = None;
            let mut filter = None;
            let mut collection_name = String::new();

            loop {
                match current {
                    LogicalPlan::Limit { input, limit: l } => {
                        limit = Some(*l);
                        current = input;
                    }
                    LogicalPlan::Offset { input, offset: o } => {
                        offset = Some(*o);
                        current = input;
                    }
                    LogicalPlan::Project { input, projections } => {
                        project = Some(projections);
                        current = input;
                    }
                    LogicalPlan::Filter { input, predicate } => {
                        filter = Some(predicate);
                        current = input;
                    }
                    LogicalPlan::Scan { collection } => {
                        collection_name = collection.clone();
                        break;
                    }
                }
            }

            let collection = db.collection::<Document>(&collection_name);
            let mut pipeline = Vec::new();

            if let Some(expr) = filter {
                if let Some(match_doc) = expr_to_match(expr) {
                    pipeline.push(doc! { "$match": match_doc });
                }
            }

            if let Some(projs) = project {
                let mut project_doc = Document::new();
                for (i, expr) in projs.iter().enumerate() {
                    let val = expr_to_project_expr(expr);
                    let field_name = if let Expression::FieldReference(_, s) = expr {
                        s.to_string()
                    } else {
                        format!("col_{}", i)
                    };
                    project_doc.insert(field_name, val);
                }
                project_doc.insert("_id", 0);
                pipeline.push(doc! { "$project": project_doc });
            }

            if let Some(o) = offset {
                pipeline.push(doc! { "$skip": o as i64 });
            }

            if let Some(l) = limit {
                pipeline.push(doc! { "$limit": l as i64 });
            }

            match collection.aggregate(pipeline).await {
                Ok(_) => {}
                Err(e) => eprintln!("Error executing {}: {}", query.name, e),
            }
        }
        _ => eprintln!("Unsupported statement type"),
    }
}

#[cfg(feature = "mongo")]
fn expr_to_match(expr: &Expression) -> Option<Document> {
    match expr {
        Expression::Binary { left, op, right } => {
            if let (Expression::FieldReference(_, f), Expression::Literal(v)) =
                (left.as_ref(), right.as_ref())
            {
                let serde_v = jsonb_to_serde(v);
                let bson_v = json_to_bson(&serde_v);
                match op {
                    BinaryOperator::Eq => Some(doc! { *f: bson_v }),
                    BinaryOperator::Gt => Some(doc! { *f: { "$gt": bson_v } }),
                    BinaryOperator::Lt => Some(doc! { *f: { "$lt": bson_v } }),
                    BinaryOperator::Gte => Some(doc! { *f: { "$gte": bson_v } }),
                    BinaryOperator::Lte => Some(doc! { *f: { "$lte": bson_v } }),
                    BinaryOperator::Neq => Some(doc! { *f: { "$ne": bson_v } }),
                }
            } else {
                None
            }
        }
        Expression::Logical { left, op, right } => {
            let l = expr_to_match(left)?;
            let r = expr_to_match(right)?;
            match op {
                LogicalOperator::Or => Some(doc! { "$or": [l, r] }),
                LogicalOperator::And => Some(doc! { "$and": [l, r] }),
            }
        }
        _ => None,
    }
}

#[cfg(feature = "mongo")]
fn expr_to_project_expr(expr: &Expression) -> Bson {
    match expr {
        Expression::FieldReference(_, s) => Bson::String(format!("${}", s)),
        Expression::Function { func, args } => {
            if args.is_empty() {
                return Bson::Null;
            }
            let arg0 = expr_to_project_expr(&args[0]);
            match func {
                ScalarFunction::Tan => Bson::Document(doc! { "$tan": arg0 }),
                // Add more if needed
                _ => Bson::Null,
            }
        }
        _ => Bson::Null,
    }
}

#[cfg(feature = "mongo")]
fn json_to_bson(v: &serde_json::Value) -> Bson {
    match v {
        serde_json::Value::Null => Bson::Null,
        serde_json::Value::Bool(b) => Bson::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Bson::Int64(u as i64)
                } else {
                    Bson::Double(u as f64)
                }
            } else {
                Bson::Null
            }
        }
        serde_json::Value::String(s) => Bson::String(s.clone()),
        serde_json::Value::Array(arr) => Bson::Array(arr.iter().map(json_to_bson).collect()),
        serde_json::Value::Object(obj) => {
            let mut doc = Document::new();
            for (k, v) in obj {
                doc.insert(k, json_to_bson(v));
            }
            Bson::Document(doc)
        }
    }
}

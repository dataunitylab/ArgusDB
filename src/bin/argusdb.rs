use async_trait::async_trait;
use clap::Parser;
use config::{Config, Environment, File};
use futures::stream;
use pgwire::api::Type;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{Level, info, span};
use tracing_subscriber;

use argusdb::db::DB;
use argusdb::parser as argus_parser;
use argusdb::query::{Statement, execute_plan};

/// ArgusDB Server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, disable_help_flag = true)]
struct Args {
    /// Host to bind to
    #[arg(short = 'h', long)]
    host: Option<String>,

    /// Port to bind to
    #[arg(short, long)]
    port: Option<u16>,

    /// Print help
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct Settings {
    #[serde(default = "default_host")]
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_memtable_threshold")]
    memtable_threshold: usize,
    #[serde(default = "default_jstable_threshold")]
    jstable_threshold: u64,
    #[serde(default = "default_jstable_dir")]
    jstable_dir: String,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    5432
}

fn default_memtable_threshold() -> usize {
    10
}

fn default_jstable_threshold() -> u64 {
    5
}

fn default_jstable_dir() -> String {
    "argus_data".to_string()
}

pub struct ArgusHandler {
    db: Arc<Mutex<DB>>,
}

impl ArgusHandler {
    fn new(db: Arc<Mutex<DB>>) -> Self {
        ArgusHandler { db }
    }
}

#[async_trait]
impl SimpleQueryHandler for ArgusHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let span = span!(Level::DEBUG, "query", query);
        let _enter = span.enter();

        let stmt = match argus_parser::parse(query) {
            Ok(s) => s,
            Err(e) => {
                return Ok(vec![Response::Error(Box::new(
                    PgWireError::ApiError(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e,
                    )))
                    .into(),
                ))]);
            }
        };

        let mut db = self.db.lock().await;

        match stmt {
            Statement::Insert {
                collection,
                documents,
            } => {
                let count = documents.len();
                for doc in documents {
                    db.insert(&collection, doc).map_err(|e| {
                        PgWireError::ApiError(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )))
                    })?;
                }
                Ok(vec![Response::Execution(Tag::new(&format!(
                    "INSERT 0 {}",
                    count
                )))])
            }
            Statement::Select(plan) => {
                let iter = execute_plan(plan, &*db).map_err(|e| {
                    PgWireError::ApiError(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e,
                    )))
                })?;

                let mut rows_data = Vec::new();
                for (_, doc) in iter {
                    rows_data.push(doc);
                }

                if rows_data.is_empty() {
                    let fields = Arc::new(vec![]);
                    let schema = Response::Query(QueryResponse::new(fields, stream::iter(vec![])));
                    return Ok(vec![schema]);
                }

                let first = &rows_data[0];
                let obj = first.as_object().unwrap();
                let fields: Vec<FieldInfo> = obj
                    .keys()
                    .map(|k| {
                        FieldInfo::new(k.clone().into(), None, None, Type::JSON, FieldFormat::Text)
                    })
                    .collect();
                let fields = Arc::new(fields);

                let mut data_rows: Vec<PgWireResult<DataRow>> = Vec::new();
                for doc in rows_data {
                    let mut encoder = DataRowEncoder::new(fields.clone());
                    let obj = doc.as_object().unwrap();
                    for field in fields.iter() {
                        let key = field.name();
                        let val = obj.get(key).unwrap_or(&serde_json::Value::Null);
                        encoder
                            .encode_field(&val.to_string())
                            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    }
                    data_rows.push(Ok(encoder.take_row()));
                }

                let row_stream = stream::iter(data_rows);
                Ok(vec![Response::Query(QueryResponse::new(
                    fields, row_stream,
                ))])
            }
            Statement::CreateCollection { collection } => {
                db.create_collection(&collection).map_err(|e| {
                    PgWireError::ApiError(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e,
                    )))
                })?;
                Ok(vec![Response::Execution(Tag::new("CREATE COLLECTION"))])
            }
            Statement::DropCollection { collection } => {
                db.drop_collection(&collection).map_err(|e| {
                    PgWireError::ApiError(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e,
                    )))
                })?;
                Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))])
            }
            Statement::ShowCollections => {
                let collections = db.show_collections();
                let fields = Arc::new(vec![FieldInfo::new(
                    "Collections".into(),
                    None,
                    None,
                    Type::VARCHAR,
                    FieldFormat::Text,
                )]);
                let mut data_rows: Vec<PgWireResult<DataRow>> = Vec::new();
                for col in collections {
                    let mut encoder = DataRowEncoder::new(fields.clone());
                    encoder
                        .encode_field(&col)
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    data_rows.push(Ok(encoder.take_row()));
                }
                let row_stream = stream::iter(data_rows);
                Ok(vec![Response::Query(QueryResponse::new(
                    fields, row_stream,
                ))])
            }
        }
    }
}

struct ArgusProcessor {
    handler: Arc<ArgusHandler>,
}

impl PgWireServerHandlers for ArgusProcessor {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(pgwire::api::NoopHandler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(pgwire::api::NoopHandler)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(pgwire::api::NoopHandler)
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut builder = Config::builder()
        .add_source(File::with_name("argusdb").required(false))
        .add_source(Environment::with_prefix("ARGUS"));

    if let Some(host) = args.host {
        builder = builder.set_override("host", host).unwrap();
    }
    if let Some(port) = args.port {
        builder = builder.set_override("port", port).unwrap();
    }

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let settings: Settings = builder.build().unwrap().try_deserialize().unwrap();

    let db = Arc::new(Mutex::new(DB::new(
        &settings.jstable_dir,
        settings.memtable_threshold,
        settings.jstable_threshold,
    )));
    let handler = Arc::new(ArgusHandler::new(db));
    let processor = Arc::new(ArgusProcessor { handler });

    let server_addr = format!("{}:{}", settings.host, settings.port);
    let listener = TcpListener::bind(&server_addr).await.unwrap();
    info!("ArgusDB server listening on {}", server_addr);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let processor = processor.clone();

        tokio::spawn(async move {
            let _ = process_socket(socket, None, processor).await;
        });
    }
}

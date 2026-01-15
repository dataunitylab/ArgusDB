use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::TcpListener;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{SimpleQueryHandler, ExtendedQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldInfo, Response, QueryResponse, Tag, FieldFormat};
use pgwire::api::{ClientInfo, PgWireServerHandlers, ErrorHandler};
use pgwire::error::{PgWireResult, PgWireError};
use pgwire::tokio::process_socket;
use pgwire::api::Type;
use pgwire::messages::data::DataRow;
use futures::stream;

pub mod schema;
pub mod storage;
pub mod log;
pub mod db;
pub mod jstable;
pub mod query;
pub mod parser;

use crate::db::DB;
use crate::parser as argus_parser;
use crate::query::{Statement, execute_plan};

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
        println!("Received query: {}", query);
        
        let stmt = match argus_parser::parse(query) {
            Ok(s) => s,
            Err(e) => return Ok(vec![Response::Error(Box::new(PgWireError::ApiError(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))).into()))]),
        };

        let mut db = self.db.lock().await;

        match stmt {
            Statement::Insert { collection: _, documents } => {
                let count = documents.len();
                for doc in documents {
                    db.insert(doc);
                }
                Ok(vec![Response::Execution(Tag::new(&format!("INSERT 0 {}", count)))])
            }
            Statement::Select(plan) => {
                let iter = execute_plan(plan, &*db);
                
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
                let fields: Vec<FieldInfo> = obj.keys().map(|k| {
                    FieldInfo::new(k.clone().into(), None, None, Type::JSON, FieldFormat::Text)
                }).collect();
                let fields = Arc::new(fields);
                
                let mut data_rows: Vec<PgWireResult<DataRow>> = Vec::new();
                for doc in rows_data {
                    let mut encoder = DataRowEncoder::new(fields.clone());
                    let obj = doc.as_object().unwrap();
                    for field in fields.iter() {
                        let key = field.name();
                        let val = obj.get(key).unwrap_or(&serde_json::Value::Null);
                        encoder.encode_field(&val.to_string()).map_err(|e| PgWireError::ApiError(Box::new(e)))?; 
                    }
                    data_rows.push(Ok(encoder.take_row()));
                }
                
                let row_stream = stream::iter(data_rows);
                Ok(vec![Response::Query(QueryResponse::new(fields, row_stream))])
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
    let db = Arc::new(Mutex::new(DB::new("argus_data")));
    let handler = Arc::new(ArgusHandler::new(db));
    let processor = Arc::new(ArgusProcessor { handler });

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("ArgusDB server listening on {}", server_addr);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let processor = processor.clone();

        tokio::spawn(async move {
            process_socket(socket, None, processor).await.expect("Failed to process socket");
        });
    }
}


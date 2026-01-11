use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug)]
pub enum Operation {
    Insert {
        doc: Value,
    },
    Update {
        id: String,
        doc: Value,
    },
    Delete {
        id: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub ts: DateTime<Utc>,
    pub op: Operation,
}

pub struct Logger {
    file: std::fs::File,
}

impl Logger {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)?;
        Ok(Logger { file })
    }

    pub fn log(&mut self, op: Operation) -> std::io::Result<()> {
        let entry = LogEntry {
            ts: Utc::now(),
            op,
        };
        let json = serde_json::to_string(&entry)?;
        writeln!(self.file, "{}", json)
    }
}

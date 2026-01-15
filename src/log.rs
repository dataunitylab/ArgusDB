use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug)]
pub enum Operation {
    Insert { id: String, doc: Value },
    Update { id: String, doc: Value },
    Delete { id: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub ts: DateTime<Utc>,
    pub op: Operation,
}

pub struct Logger {
    file: std::fs::File,
    path: PathBuf,
    rotation_threshold: u64,
}

impl Logger {
    pub fn new<P: AsRef<Path>>(path: P, rotation_threshold: u64) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Logger {
            file,
            path,
            rotation_threshold,
        })
    }

    pub fn log(&mut self, op: Operation) -> std::io::Result<()> {
        if self.file.metadata()?.len() > self.rotation_threshold {
            self.rotate()?;
        }
        let entry = LogEntry { ts: Utc::now(), op };
        let json = serde_json::to_string(&entry)?;
        writeln!(self.file, "{}", json)
    }

    pub fn rotate(&mut self) -> std::io::Result<()> {
        let new_path = self.path.with_extension("log.1");
        fs::rename(&self.path, new_path)?;
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn test_log_rotate() {
        let log_file = NamedTempFile::new().unwrap();
        let mut logger = Logger::new(log_file.path(), 1024 * 1024).unwrap();
        let op = Operation::Insert {
            id: "test-id".to_string(),
            doc: json!({"a": 1}),
        };
        logger.log(op).unwrap();

        logger.rotate().unwrap();

        let log_content = std::fs::read_to_string(log_file.path()).unwrap();
        assert!(log_content.is_empty());

        let rotated_log_path = log_file.path().with_extension("log.1");
        let rotated_log_content = std::fs::read_to_string(rotated_log_path).unwrap();
        assert!(!rotated_log_content.is_empty());
    }
}

use crate::{Value, serde_value};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use tracing::{Level, span};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    Insert {
        id: String,
        #[serde(with = "serde_value")]
        doc: Value,
    },
    Update {
        id: String,
        #[serde(with = "serde_value")]
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

pub trait Log: Send {
    fn log(&mut self, op: Operation) -> std::io::Result<()>;
    fn rotate(&mut self) -> std::io::Result<()>;
}

struct CountingWriter<'a, W> {
    inner: &'a mut W,
    count: usize,
}

impl<'a, W: Write> Write for CountingWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.count += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub struct Logger {
    file: std::io::BufWriter<std::fs::File>,
    path: PathBuf,
    rotation_threshold: u64,
    current_size: u64,
}

impl Logger {
    pub fn new<P: AsRef<Path>>(path: P, rotation_threshold: u64) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let file = std::io::BufWriter::new(file);
        // We need to seek to end to ensure current_size is correct if we just opened it?
        // OpenOptions append(true) handles writes, but for size?
        // metadata() gives file size.
        // BufWriter doesn't change that initially.
        let current_size = fs::metadata(&path)?.len();
        Ok(Logger {
            file,
            path,
            rotation_threshold,
            current_size,
        })
    }
}

impl Log for Logger {
    fn log(&mut self, op: Operation) -> std::io::Result<()> {
        let op_type = match &op {
            Operation::Insert { .. } => "insert",
            Operation::Update { .. } => "update",
            Operation::Delete { .. } => "delete",
        };
        let op_id = match &op {
            Operation::Insert { id, .. } => id,
            Operation::Update { id, .. } => id,
            Operation::Delete { id } => id,
        };
        let span = span!(Level::DEBUG, "log", op_type, op_id);
        let _enter = span.enter();

        if self.current_size > self.rotation_threshold {
            self.rotate()?;
        }
        let entry = LogEntry { ts: Utc::now(), op };

        let mut writer = CountingWriter {
            inner: &mut self.file,
            count: 0,
        };
        serde_json::to_writer(&mut writer, &entry)?;
        writer.write_all(b"\n")?;
        // Flush the BufWriter to ensure data reaches the OS cache (syscall)
        // This effectively batches the small writes from serde into one syscall per log entry.
        writer.flush()?;

        self.current_size += writer.count as u64;
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        // Ensure everything is written before rotating
        self.file.flush()?;

        let new_path = self.path.with_extension("log.1");
        fs::rename(&self.path, new_path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.file = std::io::BufWriter::new(file);
        self.current_size = 0;
        Ok(())
    }
}

pub struct NullLogger;

impl Log for NullLogger {
    fn log(&mut self, _op: Operation) -> std::io::Result<()> {
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde_to_jsonb;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn test_log_rotate() {
        let log_file = NamedTempFile::new().unwrap();
        let mut logger = Logger::new(log_file.path(), 1024 * 1024).unwrap();
        let op = Operation::Insert {
            id: "test-id".to_string(),
            doc: serde_to_jsonb(json!({"a": 1})),
        };
        logger.log(op).unwrap();

        logger.rotate().unwrap();

        let log_content = std::fs::read_to_string(log_file.path()).unwrap();
        assert!(log_content.is_empty());

        let rotated_log_path = log_file.path().with_extension("log.1");
        let rotated_log_content = std::fs::read_to_string(rotated_log_path).unwrap();
        assert!(!rotated_log_content.is_empty());
    }

    #[test]
    fn test_log_auto_rotation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log_path = temp_dir.path().join("test.log");
        // Set a very small threshold to trigger auto-rotation quickly
        let mut logger = Logger::new(&log_path, 10).unwrap();

        let op = Operation::Insert {
            id: "test-id".to_string(),
            doc: serde_to_jsonb(json!({"a": 1})),
        };

        // This log should trigger rotation if the size exceeds 10 bytes
        logger.log(op.clone()).unwrap();

        // At this point, current_size might be > 10, but rotation happens at the BEGINNING of log()
        // So we need another log to trigger it, OR the first one might have triggered it if we
        // initialized current_size differently.
        // Actually, Logger::log checks current_size > rotation_threshold BEFORE writing.

        // Write enough to definitely exceed 10 bytes
        for _ in 0..5 {
            logger.log(op.clone()).unwrap();
        }

        let rotated_log_path = log_path.with_extension("log.1");
        assert!(
            rotated_log_path.exists(),
            "Auto-rotated log file should exist"
        );

        let log_content = std::fs::read_to_string(&log_path).unwrap();
        let rotated_log_content = std::fs::read_to_string(rotated_log_path).unwrap();

        assert!(
            !rotated_log_content.is_empty(),
            "Rotated log should not be empty"
        );
        assert!(
            !log_content.is_empty(),
            "Current log should not be empty after more writes"
        );
    }
}

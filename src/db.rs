use crate::log::{LogEntry, Logger, Operation};
use crate::storage::MemTable;
use serde_json::Value;
use uuid::Uuid;

const MEMTABLE_THRESHOLD: usize = 10;

pub struct DB {
    memtable: MemTable,
    jstable_dir: String,
    jstable_count: u64,
    logger: Logger,
}

impl DB {
    pub fn new(jstable_dir: &str) -> Self {
        std::fs::create_dir_all(jstable_dir).unwrap();
        let log_path = format!("{}/argus.log", jstable_dir);
        let logger = Logger::new(&log_path, 1024 * 1024).unwrap();
        let mut memtable = MemTable::new();

        let log_content = std::fs::read_to_string(&log_path).unwrap_or_default();
        for line in log_content.lines() {
            if line.is_empty() {
                continue;
            }
            let entry: LogEntry = serde_json::from_str(line).unwrap();
            match entry.op {
                Operation::Insert { id, doc } => {
                    memtable.insert(id, doc);
                }
                Operation::Update { id, doc } => {
                    memtable.update(&id, doc);
                }
                Operation::Delete { id } => {
                    memtable.delete(&id);
                }
            }
        }

        DB {
            memtable,
            jstable_dir: jstable_dir.to_string(),
            jstable_count: 0,
            logger,
        }
    }

    pub fn insert(&mut self, doc: Value) -> String {
        if self.memtable.len() >= MEMTABLE_THRESHOLD {
            self.flush();
        }
        let id = Uuid::now_v7().to_string();
        self.logger
            .log(Operation::Insert {
                id: id.clone(),
                doc: doc.clone(),
            })
            .unwrap();
        self.memtable.insert(id.clone(), doc);
        id
    }

    pub fn delete(&mut self, id: &str) {
        self.logger
            .log(Operation::Delete { id: id.to_string() })
            .unwrap();
        self.memtable.delete(id);
    }

    pub fn update(&mut self, id: &str, doc: Value) {
        self.logger
            .log(Operation::Update {
                id: id.to_string(),
                doc: doc.clone(),
            })
            .unwrap();
        self.memtable.update(id, doc);
    }

    fn flush(&mut self) {
        let jstable_path = format!("{}/jstable-{}", self.jstable_dir, self.jstable_count);
        self.memtable.flush(&jstable_path).unwrap();
        self.jstable_count += 1;
        self.memtable = MemTable::new();
        self.logger.rotate().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn test_db_flush() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(dir.path().to_str().unwrap());

        for i in 0..MEMTABLE_THRESHOLD {
            db.insert(json!({ "a": i }));
        }
        assert_eq!(db.memtable.len(), MEMTABLE_THRESHOLD);
        assert_eq!(db.jstable_count, 0);

        db.insert(json!({"a": MEMTABLE_THRESHOLD}));
        assert_eq!(db.memtable.len(), 1);
        assert_eq!(db.jstable_count, 1);

        let jstable_path = format!("{}/jstable-0", dir.path().to_str().unwrap());
        let content = std::fs::read_to_string(jstable_path).unwrap();
        assert!(!content.is_empty());
    }

    #[test]
    fn test_log_content() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(dir.path().to_str().unwrap());
        let doc1 = json!({"a": 1});
        let id1 = db.insert(doc1.clone());

        let doc2 = json!({"b": "hello"});
        db.update(&id1, doc2.clone());

        db.delete(&id1);

        let log_path = format!("{}/argus.log", dir.path().to_str().unwrap());
        let log_content = std::fs::read_to_string(log_path).unwrap();
        let mut lines = log_content.lines();

        let entry1: crate::log::LogEntry = serde_json::from_str(lines.next().unwrap()).unwrap();
        match entry1.op {
            Operation::Insert { id, doc } => {
                assert_eq!(id, id1);
                assert_eq!(doc, doc1);
            }
            _ => panic!("Expected insert operation"),
        }

        let entry2: crate::log::LogEntry = serde_json::from_str(lines.next().unwrap()).unwrap();
        match entry2.op {
            Operation::Update { id, doc } => {
                assert_eq!(id, id1);
                assert_eq!(doc, doc2);
            }
            _ => panic!("Expected update operation"),
        }

        let entry3: crate::log::LogEntry = serde_json::from_str(lines.next().unwrap()).unwrap();
        match entry3.op {
            Operation::Delete { id } => assert_eq!(id, id1),
            _ => panic!("Expected delete operation"),
        }
    }

    #[test]
    fn test_db_recover() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(dir.path().to_str().unwrap());
        let doc1 = json!({"a": 1});
        let id1 = db.insert(doc1.clone());

        let doc2 = json!({"b": "hello"});
        let id2 = db.insert(doc2.clone());

        db.delete(&id1);

        let db2 = DB::new(dir.path().to_str().unwrap());
        assert_eq!(db2.memtable.len(), 1);
        assert_eq!(*db2.memtable.documents.get(&id2).unwrap(), doc2);
    }
}

use crate::jstable;
use crate::log::{LogEntry, Logger, Operation};
use crate::storage::MemTable;
use serde_json::{json, Value};
use std::fs;
use uuid::Uuid;

const MEMTABLE_THRESHOLD: usize = 10;
const JSTABLE_THRESHOLD: u64 = 5;

pub struct DB {
    memtable: MemTable,
    jstable_dir: String,
    jstable_count: u64,
    logger: Logger,
}

impl DB {
    pub fn new(jstable_dir: &str) -> Self {
        fs::create_dir_all(jstable_dir).unwrap();
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

        if self.jstable_count >= JSTABLE_THRESHOLD {
            self.compact();
        }
    }

    fn compact(&mut self) {
        let mut tables = Vec::new();
        for i in 0..self.jstable_count {
            let path = format!("{}/jstable-{}", self.jstable_dir, i);
            tables.push(jstable::read_jstable(&path).unwrap());
        }

        let merged_table = jstable::merge_jstables(&tables);

        for i in 0..self.jstable_count {
            let path = format!("{}/jstable-{}", self.jstable_dir, i);
            fs::remove_file(path).unwrap();
        }

        let new_path = format!("{}/jstable-0", self.jstable_dir);
        merged_table.write(&new_path).unwrap();

        self.jstable_count = 1;
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

    #[test]
    fn test_db_compaction() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(dir.path().to_str().unwrap());

        for i in 0..(MEMTABLE_THRESHOLD * JSTABLE_THRESHOLD as usize) {
            db.insert(json!({ "a": i }));
        }

        assert_eq!(db.jstable_count, JSTABLE_THRESHOLD - 1);
        db.insert(json!({ "a": 999 })); // Trigger another flush, which triggers compaction
        assert_eq!(db.jstable_count, 1);

        // Verify data is preserved
        // We inserted 0..50 (50 items) + 1 (999). 51 items total.
        // Item "0" should be in the compacted table.
        // We can't easily query DB yet (no read path implemented in DB), 
        // so we manually check the file.
        let jstable_path = format!("{}/jstable-0", dir.path().to_str().unwrap());
        let table = jstable::read_jstable(&jstable_path).unwrap();
        assert!(table.documents.len() >= 50); // It should have the 50 items from the first 5 flushes (0-49)
        // The last inserted item (999) is in memtable, not in jstable-0 yet.
    }

    #[test]
    fn test_db_compaction_with_delete() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(dir.path().to_str().unwrap());

        // 1. Insert doc to be deleted
        let id_to_delete = db.insert(json!({ "a": 100 }));
        
        // Fill memtable to force flush 1 (jstable-0)
        // 1 item already inserted. Insert 9 more to fill (total 10).
        for i in 0..9 {
            db.insert(json!({ "fill": i }));
        }
        // 11th insert triggers flush of the first 10
        db.insert(json!({ "trigger_1": 1 }));
        assert_eq!(db.jstable_count, 1); 

        // 2. Delete the doc
        // id_to_delete is in jstable-0.
        // delete adds tombstone to memtable.
        // memtable currently has "trigger_1" (1 item).
        // delete adds 1 item. len = 2.
        db.delete(&id_to_delete);
        
        // Fill memtable to force flush 2 (jstable-1)
        // Memtable len is 2. Need 8 more to fill (total 10).
        for i in 0..8 {
            db.insert(json!({ "fill_2": i }));
        }
        // 11th insert (relative to this batch) triggers flush
        db.insert(json!({ "trigger_2": 1 }));
        assert_eq!(db.jstable_count, 2); 

        // 3. Create 3 more tables to reach threshold 5
        for t in 0..3 {
            // Fill memtable (10 items)
            // Memtable has 1 item ("trigger_2" or previous trigger).
            // Need 9 more.
            for i in 0..9 {
                db.insert(json!({ "fill_more": t, "i": i }));
            }
            // Trigger flush
            db.insert(json!({ "trigger_more": t }));
        }
        // After 3rd iteration (total 5th flush), compaction triggers.
        // jstable_count goes 4 -> 5 -> 1.
        assert_eq!(db.jstable_count, 1);
        
        // 4. Verify id_to_delete is NOT in jstable-0
        let jstable_path = format!("{}/jstable-0", dir.path().to_str().unwrap());
        let table = jstable::read_jstable(&jstable_path).unwrap();
        assert!(!table.documents.contains_key(&id_to_delete));
        
        // Verify other documents exist (e.g. from flush 1)
        assert!(table.documents.len() > 40);
    }
}

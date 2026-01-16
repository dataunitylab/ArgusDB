use crate::jstable;
use crate::log::{LogEntry, Logger, Operation};
use crate::storage::MemTable;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::iter::Peekable;
use std::path::PathBuf;
use uuid::Uuid;

fn sanitize_filename(name: &str) -> String {
    let mut result = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            result.push(c);
        } else {
            result.push_str(&format!("_{:02x}", c as u32));
        }
    }
    result
}

struct Collection {
    name: String,
    memtable: MemTable,
    dir: PathBuf,
    jstable_count: u64,
    logger: Logger,
    memtable_threshold: usize,
    jstable_threshold: u64,
}

impl Collection {
    fn new(name: String, dir: PathBuf, memtable_threshold: usize, jstable_threshold: u64) -> Self {
        fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("argus.log");
        let logger = Logger::new(&log_path, 1024 * 1024).unwrap();
        let mut memtable = MemTable::new();

        let log_content = std::fs::read_to_string(&log_path).unwrap_or_default();
        for line in log_content.lines() {
            if line.is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<LogEntry>(line) {
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
        }

        // Count existing JSTables
        let mut jstable_count = 0;
        while dir.join(format!("jstable-{}", jstable_count)).exists() {
            jstable_count += 1;
        }

        Collection {
            name,
            memtable,
            dir,
            jstable_count,
            logger,
            memtable_threshold,
            jstable_threshold,
        }
    }

    #[tracing::instrument]
    fn insert(&mut self, doc: Value) -> String {
        if self.memtable.len() >= self.memtable_threshold {
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

    #[tracing::instrument]
    fn delete(&mut self, id: &str) {
        self.logger
            .log(Operation::Delete { id: id.to_string() })
            .unwrap();
        self.memtable.delete(id);
    }

    #[tracing::instrument]
    fn update(&mut self, id: &str, doc: Value) {
        self.logger
            .log(Operation::Update {
                id: id.to_string(),
                doc: doc.clone(),
            })
            .unwrap();
        self.memtable.update(id, doc);
    }

    fn flush(&mut self) {
        let jstable_path = self.dir.join(format!("jstable-{}", self.jstable_count));
        self.memtable
            .flush(jstable_path.to_str().unwrap(), self.name.clone())
            .unwrap();
        self.jstable_count += 1;
        self.memtable = MemTable::new();
        self.logger.rotate().unwrap();

        if self.jstable_count >= self.jstable_threshold {
            self.compact();
        }
    }

    fn compact(&mut self) {
        let mut tables = Vec::new();
        for i in 0..self.jstable_count {
            let path = self.dir.join(format!("jstable-{}", i));
            tables.push(jstable::read_jstable(path.to_str().unwrap()).unwrap());
        }

        let merged_table = jstable::merge_jstables(&tables);

        for i in 0..self.jstable_count {
            let path = self.dir.join(format!("jstable-{}", i));
            fs::remove_file(path).unwrap();
        }

        let new_path = self.dir.join("jstable-0");
        merged_table.write(new_path.to_str().unwrap()).unwrap();

        self.jstable_count = 1;
    }

    fn scan(&self) -> impl Iterator<Item = (String, Value)> + '_ {
        let mut sources: Vec<Peekable<Box<dyn Iterator<Item = (String, Value)>>>> = Vec::new();

        // 1. MemTable Iterator (Priority 0 - Highest)
        let mem_iter = self
            .memtable
            .documents
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()));
        sources.push((Box::new(mem_iter) as Box<dyn Iterator<Item = (String, Value)>>).peekable());

        // 2. JSTable Iterators (Newer to Older)
        for i in (0..self.jstable_count).rev() {
            let path = self.dir.join(format!("jstable-{}", i));
            if let Ok(iter) = jstable::JSTableIterator::new(path.to_str().unwrap()) {
                let iter = iter.map(|r| r.unwrap());
                sources
                    .push((Box::new(iter) as Box<dyn Iterator<Item = (String, Value)>>).peekable());
            }
        }

        MergedIterator { sources }
    }
}

impl Debug for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Collection")
            .field("name", &self.name)
            .field("dir", &self.dir)
            .finish()
    }
}

pub struct DB {
    root_dir: PathBuf,
    collections: HashMap<String, Collection>,
    memtable_threshold: usize,
    jstable_threshold: u64,
}

struct MergedIterator<'a> {
    sources: Vec<Peekable<Box<dyn Iterator<Item = (String, Value)> + 'a>>>,
}

impl<'a> Iterator for MergedIterator<'a> {
    type Item = (String, Value);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Find min_id
            let mut min_id: Option<String> = None;

            for source in &mut self.sources {
                if let Some((id, _)) = source.peek() {
                    match &min_id {
                        None => min_id = Some(id.clone()),
                        Some(current_min) => {
                            if id < current_min {
                                min_id = Some(id.clone());
                            }
                        }
                    }
                }
            }

            let min_id = min_id?; // If None, all exhausted

            // Consume from sources
            let mut result: Option<Value> = None;

            for source in &mut self.sources {
                let is_match = if let Some((id, _)) = source.peek() {
                    id == &min_id
                } else {
                    false
                };

                if is_match {
                    let (_, doc) = source.next().unwrap();
                    if result.is_none() {
                        // First match (highest priority)
                        result = Some(doc);
                    }
                    // Else: ignored (shadowed)
                }
            }

            if let Some(doc) = result
                && !doc.is_null()
            {
                return Some((min_id, doc));
            }
            // If null (tombstone), loop again
        }
    }
}

impl DB {
    pub fn new(root_dir: &str, memtable_threshold: usize, jstable_threshold: u64) -> Self {
        fs::create_dir_all(root_dir).unwrap();
        let mut collections = HashMap::new();

        if let Ok(entries) = fs::read_dir(root_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if entry.path().is_dir() {
                        let dir_path = entry.path();

                        // Try to find collection name from JSTable-0
                        let jstable_path = dir_path.join("jstable-0");
                        let col_name = if jstable_path.exists() {
                            if let Ok(iter) =
                                jstable::JSTableIterator::new(jstable_path.to_str().unwrap())
                            {
                                Some(iter.collection)
                            } else {
                                None
                            }
                        } else {
                            // Fallback to directory name (sanitized) if no jstable
                            entry.file_name().to_str().map(|s| s.to_string())
                        };

                        if let Some(name) = col_name {
                            let collection = Collection::new(
                                name.clone(),
                                dir_path,
                                memtable_threshold,
                                jstable_threshold,
                            );
                            collections.insert(name, collection);
                        }
                    }
                }
            }
        }

        DB {
            root_dir: PathBuf::from(root_dir),
            collections,
            memtable_threshold,
            jstable_threshold,
        }
    }

    fn get_collection(&mut self, name: &str) -> &mut Collection {
        self.collections.entry(name.to_string()).or_insert_with(|| {
            let safe_name = sanitize_filename(name);
            let col_dir = self.root_dir.join(safe_name);
            Collection::new(
                name.to_string(),
                col_dir,
                self.memtable_threshold,
                self.jstable_threshold,
            )
        })
    }

    pub fn insert(&mut self, collection: &str, doc: Value) -> String {
        self.get_collection(collection).insert(doc)
    }

    pub fn delete(&mut self, collection: &str, id: &str) {
        self.get_collection(collection).delete(id);
    }

    pub fn update(&mut self, collection: &str, id: &str, doc: Value) {
        self.get_collection(collection).update(id, doc);
    }

    pub fn scan(&self, collection: &str) -> Box<dyn Iterator<Item = (String, Value)> + '_> {
        if let Some(col) = self.collections.get(collection) {
            Box::new(col.scan())
        } else {
            Box::new(std::iter::empty())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    const MEMTABLE_THRESHOLD: usize = 10;
    const JSTABLE_THRESHOLD: u64 = 5;

    #[test]
    fn test_db_flush() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
        );

        for i in 0..MEMTABLE_THRESHOLD {
            db.insert("test", json!({ "a": i }));
        }
        let col = db.collections.get("test").unwrap();
        assert_eq!(col.memtable.len(), MEMTABLE_THRESHOLD);
        assert_eq!(col.jstable_count, 0);

        db.insert("test", json!({"a": MEMTABLE_THRESHOLD}));
        let col = db.collections.get("test").unwrap();
        assert_eq!(col.memtable.len(), 1);
        assert_eq!(col.jstable_count, 1);

        let jstable_path = col.dir.join("jstable-0");
        let table = jstable::read_jstable(jstable_path.to_str().unwrap()).unwrap();
        assert_eq!(table.documents.len(), MEMTABLE_THRESHOLD);
        assert_eq!(table.collection, "test");
    }

    #[test]
    fn test_log_content() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
        );
        let doc1 = json!({"a": 1});
        let id1 = db.insert("test", doc1.clone());

        let doc2 = json!({"b": "hello"});
        db.update("test", &id1, doc2.clone());

        db.delete("test", &id1);

        let col = db.collections.get("test").unwrap();
        let log_path = col.dir.join("argus.log");
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
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
        );
        let doc1 = json!({"a": 1});
        let id1 = db.insert("test", doc1.clone());

        let doc2 = json!({"b": "hello"});
        let id2 = db.insert("test", doc2.clone());

        db.delete("test", &id1);

        // Recover by creating new DB instance pointed to same dir
        let db2 = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
        );
        // "test" should be loaded if it persisted JSTable or fallback to dir name
        let col = db2.collections.get("test").unwrap();

        assert_eq!(col.memtable.len(), 2);
        assert_eq!(*col.memtable.documents.get(&id2).unwrap(), doc2);
        assert!(col.memtable.documents.get(&id1).unwrap().is_null());
    }

    #[test]
    fn test_db_compaction() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
        );

        for i in 0..(MEMTABLE_THRESHOLD * JSTABLE_THRESHOLD as usize) {
            db.insert("test", json!({ "a": i }));
        }

        let col = db.collections.get("test").unwrap();
        assert_eq!(col.jstable_count, JSTABLE_THRESHOLD - 1);
        db.insert("test", json!({ "a": 999 }));

        let col = db.collections.get("test").unwrap();
        assert_eq!(col.jstable_count, 1);
    }

    #[test]
    fn test_db_compaction_with_delete() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
        );

        let id_to_delete = db.insert("test", json!({ "a": 100 }));

        for i in 0..9 {
            db.insert("test", json!({ "fill": i }));
        }
        db.insert("test", json!({ "trigger_1": 1 }));

        let col = db.collections.get("test").unwrap();
        assert_eq!(col.jstable_count, 1);

        db.delete("test", &id_to_delete);

        for i in 0..8 {
            db.insert("test", json!({ "fill_2": i }));
        }
        db.insert("test", json!({ "trigger_2": 1 }));

        let col = db.collections.get("test").unwrap();
        assert_eq!(col.jstable_count, 2);

        for t in 0..3 {
            for i in 0..9 {
                db.insert("test", json!({ "fill_more": t, "i": i }));
            }
            db.insert("test", json!({ "trigger_more": t }));
        }

        let col = db.collections.get("test").unwrap();
        assert_eq!(col.jstable_count, 1);

        let jstable_path = col.dir.join("jstable-0");
        let table = jstable::read_jstable(jstable_path.to_str().unwrap()).unwrap();
        assert!(!table.documents.contains_key(&id_to_delete));
        assert!(table.documents.len() > 40);
    }

    #[test]
    fn test_db_scan() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
        );

        for i in 0..MEMTABLE_THRESHOLD {
            db.insert("test", json!({"val": i}));
        }
        db.insert("test", json!({"val": 10}));

        let results: HashMap<String, Value> = db.scan("test").collect();
        assert_eq!(results.len(), 11);
    }

    #[test]
    fn test_sanitize() {
        assert_eq!(sanitize_filename("valid"), "valid");
        assert_eq!(sanitize_filename("foo/bar"), "foo_2fbar");
        assert_eq!(sanitize_filename("test.1"), "test_2e1");
    }
}

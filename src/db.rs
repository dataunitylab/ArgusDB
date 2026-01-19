use crate::jstable;
use crate::log::{Log, LogEntry, Logger, NullLogger, Operation};
use crate::storage::MemTable;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::iter::Peekable;
use std::path::PathBuf;
use uuid::Uuid;
use xorf::{BinaryFuse8, Filter};

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
    pub memtable: MemTable,
    dir: PathBuf,
    jstable_count: u64,
    logger: Box<dyn Log>,
    memtable_threshold: usize,
    jstable_threshold: u64,
    filters: Vec<BinaryFuse8>,
}

impl Collection {
    fn new(
        name: String,
        dir: PathBuf,
        memtable_threshold: usize,
        jstable_threshold: u64,
        log_rotation_threshold: Option<u64>,
    ) -> Self {
        fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("argus.log");
        let logger: Box<dyn Log> = if let Some(threshold) = log_rotation_threshold {
            Box::new(Logger::new(&log_path, threshold).unwrap())
        } else {
            Box::new(NullLogger)
        };
        let memtable = MemTable::new();
        // Count existing JSTables and load filters
        let mut jstable_count = 0;
        let mut filters = Vec::new();
        // Check for .summary file to confirm JSTable existence
        while dir
            .join(format!("jstable-{}.summary", jstable_count))
            .exists()
        {
            let path = dir.join(format!("jstable-{}", jstable_count));
            if let Ok(filter) = jstable::read_filter(path.to_str().unwrap()) {
                filters.push(filter);
            } else {
                panic!("Failed to read filter for jstable-{}", jstable_count);
            }
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
            filters,
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

        // Load the new filter
        let filter = jstable::read_filter(jstable_path.to_str().unwrap()).unwrap();
        self.filters.push(filter);

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
            let base_path = self.dir.join(format!("jstable-{}", i));
            let summary_path = format!("{}.summary", base_path.to_str().unwrap());
            let data_path = format!("{}.data", base_path.to_str().unwrap());
            fs::remove_file(summary_path).unwrap();
            fs::remove_file(data_path).unwrap();
        }

        let new_path = self.dir.join("jstable-0");
        merged_table.write(new_path.to_str().unwrap()).unwrap();

        // Reset filters
        self.filters.clear();
        let filter = jstable::read_filter(new_path.to_str().unwrap()).unwrap();
        self.filters.push(filter);

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

    fn get(&self, id: &str) -> Option<Value> {
        // 1. Check MemTable
        if let Some(doc) = self.memtable.documents.get(id) {
            if doc.is_null() {
                return None; // Tombstone
            }
            return Some(doc.clone());
        }

        // 2. Check JSTables (Newer to Older)
        let hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            id.hash(&mut hasher);
            hasher.finish()
        };

        for i in (0..self.jstable_count).rev() {
            if let Some(filter) = self.filters.get(i as usize) {
                if filter.contains(&hash) {
                    // Possible match, scan the table
                    let path = self.dir.join(format!("jstable-{}", i));
                    if let Ok(iter) = jstable::JSTableIterator::new(path.to_str().unwrap()) {
                        for res in iter {
                            if let Ok((rid, doc)) = res {
                                if rid == id {
                                    if doc.is_null() {
                                        return None; // Tombstone
                                    }
                                    return Some(doc);
                                }
                            }
                        }
                    }
                }
            }
        }

        None
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
    log_rotation_threshold: Option<u64>,
}

impl DB {
    pub fn new(
        root_dir: &str,
        memtable_threshold: usize,
        jstable_threshold: u64,
        log_rotation_threshold: Option<u64>,
    ) -> Self {
        fs::create_dir_all(root_dir).unwrap();
        let mut collections = HashMap::new();

        if let Ok(entries) = fs::read_dir(root_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if entry.path().is_dir() {
                        let dir_path = entry.path();

                        // Try to find collection name from JSTable-0
                        let jstable_base_path = dir_path.join("jstable-0");
                        let jstable_summary_path = dir_path.join("jstable-0.summary");
                        let col_name = if jstable_summary_path.exists() {
                            if let Ok(iter) =
                                jstable::JSTableIterator::new(jstable_base_path.to_str().unwrap())
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
                            let mut collection = Collection::new(
                                name.clone(),
                                dir_path.clone(), // Clone dir_path for collection
                                memtable_threshold,
                                jstable_threshold,
                                log_rotation_threshold,
                            );

                            if log_rotation_threshold.is_some() {
                                let log_path = dir_path.join("argus.log");
                                let log_content =
                                    std::fs::read_to_string(&log_path).unwrap_or_default();
                                for line in log_content.lines() {
                                    if line.is_empty() {
                                        continue;
                                    }
                                    if let Ok(entry) = serde_json::from_str::<LogEntry>(line) {
                                        match entry.op {
                                            Operation::Insert { id, doc } => {
                                                collection.memtable.insert(id, doc);
                                            }
                                            Operation::Update { id, doc } => {
                                                collection.memtable.update(&id, doc);
                                            }
                                            Operation::Delete { id } => {
                                                collection.memtable.delete(&id);
                                            }
                                        }
                                    }
                                }
                            }

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
            log_rotation_threshold,
        }
    }

    fn get_collection_mut(&mut self, name: &str) -> Result<&mut Collection, String> {
        self.collections
            .get_mut(name)
            .ok_or_else(|| format!("Collection '{}' not found", name))
    }

    fn get_collection(&self, name: &str) -> Result<&Collection, String> {
        self.collections
            .get(name)
            .ok_or_else(|| format!("Collection '{}' not found", name))
    }

    pub fn create_collection(&mut self, name: &str) -> Result<(), String> {
        if self.collections.contains_key(name) {
            return Err(format!("Collection '{}' already exists", name));
        }
        let safe_name = sanitize_filename(name);
        let col_dir = self.root_dir.join(safe_name);
        let collection = Collection::new(
            name.to_string(),
            col_dir,
            self.memtable_threshold,
            self.jstable_threshold,
            self.log_rotation_threshold,
        );
        self.collections.insert(name.to_string(), collection);
        Ok(())
    }

    pub fn drop_collection(&mut self, name: &str) -> Result<(), String> {
        if let Some(collection) = self.collections.remove(name) {
            fs::remove_dir_all(collection.dir).map_err(|e| e.to_string())
        } else {
            Err(format!("Collection '{}' not found", name))
        }
    }

    pub fn show_collections(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    pub fn insert(&mut self, collection: &str, doc: Value) -> Result<String, String> {
        self.get_collection_mut(collection).map(|c| c.insert(doc))
    }

    pub fn delete(&mut self, collection: &str, id: &str) -> Result<(), String> {
        self.get_collection_mut(collection).map(|c| c.delete(id))
    }

    pub fn update(&mut self, collection: &str, id: &str, doc: Value) -> Result<(), String> {
        self.get_collection_mut(collection)
            .map(|c| c.update(id, doc))
    }

    pub fn scan(
        &self,
        collection: &str,
    ) -> Result<Box<dyn Iterator<Item = (String, Value)> + '_>, String> {
        self.get_collection(collection)
            .map(|c| Box::new(c.scan()) as Box<dyn Iterator<Item = (String, Value)> + '_>)
    }

    pub fn get(&self, collection: &str, id: &str) -> Result<Option<Value>, String> {
        self.get_collection(collection).map(|c| c.get(id))
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
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();

        for i in 0..MEMTABLE_THRESHOLD {
            db.insert("test", json!({ "a": i })).unwrap();
        }
        let col = db.collections.get("test").unwrap();
        assert_eq!(col.memtable.len(), MEMTABLE_THRESHOLD);
        assert_eq!(col.jstable_count, 0);

        db.insert("test", json!({"a": MEMTABLE_THRESHOLD})).unwrap();
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
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();
        let doc1 = json!({"a": 1});
        let id1 = db.insert("test", doc1.clone()).unwrap();

        let doc2 = json!({"b": "hello"});
        db.update("test", &id1, doc2.clone()).unwrap();

        db.delete("test", &id1).unwrap();

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
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();
        let doc1 = json!({"a": 1});
        let id1 = db.insert("test", doc1.clone()).unwrap();

        let doc2 = json!({"b": "hello"});
        let id2 = db.insert("test", doc2.clone()).unwrap();

        db.delete("test", &id1).unwrap();

        let db2 = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
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
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();

        for i in 0..(MEMTABLE_THRESHOLD * JSTABLE_THRESHOLD as usize) {
            db.insert("test", json!({ "a": i })).unwrap();
        }

        let col = db.collections.get("test").unwrap();
        assert_eq!(col.jstable_count, JSTABLE_THRESHOLD - 1);
        db.insert("test", json!({ "a": 999 })).unwrap();

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
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();
        let id_to_delete = db.insert("test", json!({ "a": 100 })).unwrap();

        for i in 0..9 {
            db.insert("test", json!({ "fill": i })).unwrap();
        }
        db.insert("test", json!({ "trigger_1": 1 })).unwrap();

        let col = db.collections.get("test").unwrap();
        assert_eq!(col.jstable_count, 1);

        db.delete("test", &id_to_delete).unwrap();

        for i in 0..8 {
            db.insert("test", json!({ "fill_2": i })).unwrap();
        }
        db.insert("test", json!({ "trigger_2": 1 })).unwrap();

        let col = db.collections.get("test").unwrap();
        assert_eq!(col.jstable_count, 2);

        for t in 0..3 {
            for i in 0..9 {
                db.insert("test", json!({ "fill_more": t, "i": i }))
                    .unwrap();
            }
            db.insert("test", json!({ "trigger_more": t })).unwrap();
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
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();

        for i in 0..MEMTABLE_THRESHOLD {
            db.insert("test", json!({"val": i})).unwrap();
        }
        db.insert("test", json!({"val": 10})).unwrap();

        let results: HashMap<String, Value> = db.scan("test").unwrap().collect();
        assert_eq!(results.len(), 11);
    }

    #[test]
    fn test_sanitize() {
        assert_eq!(sanitize_filename("valid"), "valid");
        assert_eq!(sanitize_filename("foo/bar"), "foo_2fbar");
        assert_eq!(sanitize_filename("test.1"), "test_2e1");
    }

    #[test]
    fn test_create_collection() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();
        assert!(db.collections.contains_key("test"));
    }

    #[test]
    fn test_create_collection_already_exists() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();
        let res = db.create_collection("test");
        assert!(res.is_err());
    }

    #[test]
    fn test_drop_collection() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();
        assert!(db.collections.contains_key("test"));
        db.drop_collection("test").unwrap();
        assert!(!db.collections.contains_key("test"));
    }

    #[test]
    fn test_drop_collection_not_found() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        let res = db.drop_collection("test");
        assert!(res.is_err());
    }

    #[test]
    fn test_show_collections() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        db.create_collection("test1").unwrap();
        db.create_collection("test2").unwrap();
        let collections = db.show_collections();
        assert_eq!(collections.len(), 2);
        assert!(collections.contains(&"test1".to_string()));
        assert!(collections.contains(&"test2".to_string()));
    }

    #[test]
    fn test_insert_into_non_existent_collection() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        let res = db.insert("test", json!({ "a": 1 }));
        assert!(res.is_err());
    }

    #[test]
    fn test_db_load_collections_on_startup() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();

        let db2 = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        assert!(db2.collections.contains_key("test"));
    }

    #[test]
    fn test_db_get() {
        let dir = tempdir().unwrap();
        let mut db = DB::new(
            dir.path().to_str().unwrap(),
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();
        let id = db.insert("test", json!({ "a": 1 })).unwrap();

        let doc = db.get("test", &id).unwrap().unwrap();
        assert_eq!(doc, json!({ "a": 1 }));

        // Flush to force creation of JSTable
        for i in 0..MEMTABLE_THRESHOLD {
            db.insert("test", json!({ "fill": i })).unwrap();
        }

        let doc = db.get("test", &id).unwrap().unwrap();
        assert_eq!(doc, json!({ "a": 1 }));

        assert!(db.get("test", "non-existent").unwrap().is_none());
    }
}

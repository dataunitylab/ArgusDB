use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use xorf::BinaryFuse8;

pub struct JSTable {
    pub timestamp: u64,
    pub collection: String,
    pub schema: Schema,
    pub documents: BTreeMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
struct JSTableHeader {
    timestamp: u64,
    collection: String,
    schema: Schema,
}

impl JSTable {
    pub fn new(
        timestamp: u64,
        collection: String,
        schema: Schema,
        documents: BTreeMap<String, Value>,
    ) -> Self {
        JSTable {
            timestamp,
            collection,
            schema,
            documents,
        }
    }

    pub fn write(&self, path: &str, index_threshold: u64) -> io::Result<()> {
        let summary_path = format!("{}.summary", path);
        let data_path = format!("{}.data", path);

        let mut summary_file = File::create(summary_path)?;
        let mut data_file = File::create(data_path)?;

        // Write Header to summary
        let header = JSTableHeader {
            timestamp: self.timestamp,
            collection: self.collection.clone(),
            schema: self.schema.clone(),
        };
        // Serialize header using jsonb
        let header_blob = jsonb::to_owned_jsonb(&header)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let header_bytes = header_blob.to_vec();
        let header_len = header_bytes.len() as u32;
        summary_file.write_all(&header_len.to_le_bytes())?;
        summary_file.write_all(&header_bytes)?;

        // Write Filter to summary
        let keys: Vec<u64> = self
            .documents
            .keys()
            .map(|k| {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                k.hash(&mut hasher);
                hasher.finish()
            })
            .collect();
        let filter = BinaryFuse8::try_from(&keys).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Failed to create XOR filter")
        })?;
        // Use serde_json for filter serialization
        let filter_bytes = serde_json::to_vec(&filter)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let filter_len = filter_bytes.len() as u32;
        summary_file.write_all(&filter_len.to_le_bytes())?;
        summary_file.write_all(&filter_bytes)?;

        // Write Documents to data and build index
        let mut index: Vec<(String, u64)> = Vec::new();
        let mut current_offset: u64 = 0;
        let mut bytes_since_last_index: u64 = 0;
        let mut first = true;

        for (id, doc) in &self.documents {
            // Add index entry if needed
            if first || bytes_since_last_index >= index_threshold {
                index.push((id.clone(), current_offset));
                bytes_since_last_index = 0;
                first = false;
            }

            let record: (String, &Value) = (id.clone(), doc);
            let record_blob = jsonb::to_owned_jsonb(&record)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let record_bytes = record_blob.to_vec();
            let record_len = record_bytes.len() as u32;

            data_file.write_all(&record_len.to_le_bytes())?;
            data_file.write_all(&record_bytes)?;

            let written = 4 + record_bytes.len() as u64;
            current_offset += written;
            bytes_since_last_index += written;
        }

        // Write Index to summary
        let index_bytes = serde_json::to_vec(&index)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let index_len = index_bytes.len() as u32;
        summary_file.write_all(&index_len.to_le_bytes())?;
        summary_file.write_all(&index_bytes)?;

        Ok(())
    }
}

pub struct JSTableIterator {
    reader: BufReader<File>,
    pub timestamp: u64,
    pub collection: String,
    pub schema: Schema,
}

impl JSTableIterator {
    pub fn new(path: &str) -> io::Result<Self> {
        let summary_path = format!("{}.summary", path);
        let data_path = format!("{}.data", path);

        let summary_file = File::open(summary_path)?;
        let mut summary_reader = BufReader::new(summary_file);

        // Read Header Length from summary
        let mut len_buf = [0u8; 4];
        summary_reader.read_exact(&mut len_buf)?;
        let header_len = u32::from_le_bytes(len_buf) as usize;

        // Read Header Blob from summary
        let mut header_blob = vec![0u8; header_len];
        summary_reader.read_exact(&mut header_blob)?;

        let header_val = jsonb::from_slice(&header_blob)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Convert jsonb::Value -> String -> T
        let header_str = header_val.to_string();
        let header: JSTableHeader = serde_json::from_str(&header_str)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // We don't need to read the filter or index here

        let data_file = File::open(data_path)?;
        let data_reader = BufReader::new(data_file);

        Ok(Self {
            reader: data_reader,
            timestamp: header.timestamp,
            collection: header.collection,
            schema: header.schema,
        })
    }

    pub fn seek(&mut self, offset: u64) -> io::Result<()> {
        self.reader.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

impl Iterator for JSTableIterator {
    type Item = io::Result<(String, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(_) => {
                let record_len = u32::from_le_bytes(len_buf) as usize;
                let mut record_blob = vec![0u8; record_len];
                if let Err(e) = self.reader.read_exact(&mut record_blob) {
                    return Some(Err(e));
                }

                let record_val = match jsonb::from_slice(&record_blob)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
                {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };
                let record_str = record_val.to_string();
                let record: (String, Value) = match serde_json::from_str(&record_str)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
                {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };

                Some(Ok(record))
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub fn read_jstable(path: &str) -> io::Result<JSTable> {
    let iterator = JSTableIterator::new(path)?;
    let timestamp = iterator.timestamp;
    let collection = iterator.collection.clone();
    let schema = iterator.schema.clone();

    let mut documents = BTreeMap::new();
    for result in iterator {
        let (id, doc) = result?;
        documents.insert(id, doc);
    }

    Ok(JSTable {
        timestamp,
        collection,
        schema,
        documents,
    })
}

pub fn read_filter(path: &str) -> io::Result<BinaryFuse8> {
    let summary_path = format!("{}.summary", path);
    let file = File::open(summary_path)?;
    let mut reader = BufReader::new(file);

    // Read Header Length
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let header_len = u32::from_le_bytes(len_buf) as usize;

    // Skip Header Blob
    io::copy(
        &mut reader.by_ref().take(header_len as u64),
        &mut io::sink(),
    )?;

    // Read Filter Length
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let filter_len = u32::from_le_bytes(len_buf) as usize;

    // Read Filter Blob
    let mut filter_blob = vec![0u8; filter_len];
    reader.read_exact(&mut filter_blob)?;

    // Deserialize using serde_json
    let filter: BinaryFuse8 = serde_json::from_slice(&filter_blob)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(filter)
}

pub fn read_index(path: &str) -> io::Result<Vec<(String, u64)>> {
    let summary_path = format!("{}.summary", path);
    let file = File::open(summary_path)?;
    let mut reader = BufReader::new(file);

    // Read Header Length
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let header_len = u32::from_le_bytes(len_buf) as usize;

    // Skip Header Blob
    io::copy(
        &mut reader.by_ref().take(header_len as u64),
        &mut io::sink(),
    )?;

    // Read Filter Length
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let filter_len = u32::from_le_bytes(len_buf) as usize;

    // Skip Filter Blob
    io::copy(
        &mut reader.by_ref().take(filter_len as u64),
        &mut io::sink(),
    )?;

    // Read Index Length
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let index_len = u32::from_le_bytes(len_buf) as usize;

    // Read Index Blob
    let mut index_blob = vec![0u8; index_len];
    reader.read_exact(&mut index_blob)?;

    // Deserialize
    let index: Vec<(String, u64)> = serde_json::from_slice(&index_blob)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(index)
}

pub fn merge_jstables(tables: &[JSTable]) -> JSTable {
    let mut sorted_tables: Vec<&JSTable> = tables.iter().collect();
    sorted_tables.sort_by_key(|t| t.timestamp);

    let mut merged_schema = Schema::new(crate::schema::SchemaType::Object);
    let mut merged_documents = BTreeMap::new();
    let mut max_timestamp = 0;

    let collection = if let Some(first) = tables.first() {
        first.collection.clone()
    } else {
        String::new()
    };

    for table in sorted_tables {
        if table.timestamp > max_timestamp {
            max_timestamp = table.timestamp;
        }
        merged_schema.merge(table.schema.clone());
        for (id, doc) in &table.documents {
            merged_documents.insert(id.clone(), doc.clone());
        }
    }

    merged_documents.retain(|_, v| !v.is_null());

    JSTable::new(max_timestamp, collection, merged_schema, merged_documents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaType;
    use serde_json::json;
    use tempfile::tempdir;
    use xorf::Filter;

    #[test]
    fn test_read_jstable() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema {
            types: vec![SchemaType::Object],
            properties: Some(BTreeMap::from([(
                "a".to_string(),
                Schema::new(SchemaType::Integer),
            )])),
            items: None,
        };
        let mut documents = BTreeMap::new();
        documents.insert("id1".to_string(), json!({"a": 1}));
        documents.insert("id2".to_string(), json!({"a": 2}));
        let jstable = JSTable::new(
            12345,
            "test_col".to_string(),
            schema.clone(),
            documents.clone(),
        );

        let dir = tempdir()?;
        let file_path = dir.path().join("test_table");
        jstable.write(file_path.to_str().unwrap(), 1024).unwrap();

        let read_table = read_jstable(file_path.to_str().unwrap()).unwrap();

        assert_eq!(read_table.timestamp, 12345);
        assert_eq!(read_table.collection, "test_col");
        assert_eq!(read_table.schema.types, vec![SchemaType::Object]);
        assert_eq!(read_table.documents.len(), 2);
        assert_eq!(*read_table.documents.get("id1").unwrap(), json!({"a": 1}));
        assert_eq!(*read_table.documents.get("id2").unwrap(), json!({"a": 2}));
        Ok(())
    }

    #[test]
    fn test_jstable_iterator() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema {
            types: vec![SchemaType::Object],
            properties: Some(BTreeMap::from([(
                "a".to_string(),
                Schema::new(SchemaType::Integer),
            )])),
            items: None,
        };
        let mut documents = BTreeMap::new();
        documents.insert("id1".to_string(), json!({"a": 1}));
        documents.insert("id2".to_string(), json!({"a": 2}));
        let jstable = JSTable::new(
            12345,
            "test_col".to_string(),
            schema.clone(),
            documents.clone(),
        );

        let dir = tempdir()?;
        let file_path = dir.path().join("test_table");
        jstable.write(file_path.to_str().unwrap(), 1024).unwrap();

        let iterator = JSTableIterator::new(file_path.to_str().unwrap())?;
        assert_eq!(iterator.timestamp, 12345);
        assert_eq!(iterator.collection, "test_col");

        let mut count = 0;
        let mut ids = Vec::new();
        for result in iterator {
            let (id, doc) = result?;
            count += 1;
            ids.push(id);
            assert!(doc == json!({"a": 1}) || doc == json!({"a": 2}));
        }
        assert_eq!(count, 2);
        assert!(ids.contains(&"id1".to_string()));
        assert!(ids.contains(&"id2".to_string()));

        Ok(())
    }

    #[test]
    fn test_read_filter() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema {
            types: vec![SchemaType::Object],
            properties: Some(BTreeMap::from([(
                "a".to_string(),
                Schema::new(SchemaType::Integer),
            )])),
            items: None,
        };
        let mut documents = BTreeMap::new();
        documents.insert("id1".to_string(), json!({"a": 1}));
        documents.insert("id2".to_string(), json!({"a": 2}));
        let jstable = JSTable::new(
            12345,
            "test_col".to_string(),
            schema.clone(),
            documents.clone(),
        );

        let dir = tempdir()?;
        let file_path = dir.path().join("test_table");
        jstable.write(file_path.to_str().unwrap(), 1024).unwrap();

        let filter = read_filter(file_path.to_str().unwrap())?;

        // Helper to hash string for filter check
        let hash = |s: &str| {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            s.hash(&mut hasher);
            hasher.finish()
        };

        assert!(filter.contains(&hash("id1")));
        assert!(filter.contains(&hash("id2")));
        assert!(!filter.contains(&hash("id3")));

        Ok(())
    }

    #[test]
    fn test_merge_jstables_conflict_resolution() {
        let schema = Schema::new(SchemaType::Object);

        let mut docs1 = BTreeMap::new();
        docs1.insert("id1".to_string(), json!({"v": 1}));
        let t1 = JSTable::new(100, "test_col".to_string(), schema.clone(), docs1);

        let mut docs2 = BTreeMap::new();
        docs2.insert("id1".to_string(), json!({"v": 2}));
        let t2 = JSTable::new(200, "test_col".to_string(), schema.clone(), docs2);

        // Case 1: t1 (older) then t2 (newer) in the slice
        let merged = merge_jstables(&[t1, t2]);
        assert_eq!(*merged.documents.get("id1").unwrap(), json!({"v": 2}));
        assert_eq!(merged.timestamp, 200);
        assert_eq!(merged.collection, "test_col");

        // Case 2: Reverse order
        let mut docs1 = BTreeMap::new();
        docs1.insert("id1".to_string(), json!({"v": 1}));
        let t1b = JSTable::new(100, "test_col".to_string(), schema.clone(), docs1);

        let mut docs2 = BTreeMap::new();
        docs2.insert("id1".to_string(), json!({"v": 2}));
        let t2b = JSTable::new(200, "test_col".to_string(), schema.clone(), docs2);

        let merged_reverse = merge_jstables(&[t2b, t1b]);
        assert_eq!(
            *merged_reverse.documents.get("id1").unwrap(),
            json!({"v": 2})
        );
        assert_eq!(merged_reverse.timestamp, 200);
    }

    #[test]
    fn test_jstable_keys_sorted_on_disk() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(SchemaType::Object);
        let mut documents = BTreeMap::new();
        // Insert keys in non-sorted order (BTreeMap will sort them)
        documents.insert("c".to_string(), json!(3));
        documents.insert("a".to_string(), json!(1));
        documents.insert("b".to_string(), json!(2));

        let jstable = JSTable::new(123, "sorted_test".to_string(), schema, documents);

        let dir = tempdir()?;
        let file_path = dir.path().join("test_table");
        jstable.write(file_path.to_str().unwrap(), 1024)?;

        let iterator = JSTableIterator::new(file_path.to_str().unwrap())?;
        let keys: Vec<String> = iterator.map(|r| r.unwrap().0).collect();

        assert_eq!(keys, vec!["a", "b", "c"]);
        Ok(())
    }

    #[test]
    fn test_read_index() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(SchemaType::Object);
        let mut documents = BTreeMap::new();
        // Insert enough data to trigger indexing (threshold 1024 bytes)
        // Each entry: 4 bytes length + record bytes
        // Record: ["id", "val..."]
        // We want at least one entry after the first one.

        let large_val = "x".repeat(500); // ~500 bytes
        documents.insert("a".to_string(), json!(large_val));
        documents.insert("b".to_string(), json!(large_val));
        documents.insert("c".to_string(), json!(large_val));
        // a: offset 0. write ~500+ -> offset ~500+.
        // b: offset ~500+. bytes_written since a ~ 500+. < 1024.
        // c: offset ~1000+. bytes_written since a ~ 1000+. >= 1024?
        // Let's make it larger.
        let larger_val = "x".repeat(1100);
        documents.insert("d".to_string(), json!(larger_val));
        documents.insert("e".to_string(), json!(1));

        let jstable = JSTable::new(123, "idx_test".to_string(), schema, documents);
        let dir = tempdir()?;
        let path = dir.path().join("idx_table");
        jstable.write(path.to_str().unwrap(), 1024)?;

        let index = read_index(path.to_str().unwrap())?;

        // Should contain at least "a" (first) and "e" (after "d" which is large)
        // actually "d" is ~1100.
        // a (0), b (large), c (large), d (larger), e (1)
        // sorted: a, b, c, d, e

        // "a": offset 0.
        // write "a" (large). bytes=1100.
        // next is "b". bytes_since >= 1024. so "b" is indexed?
        // Logic:
        // if first || bytes_since >= 1024 { push; bytes=0 }
        // "a": first. push ("a", 0). bytes=0.
        // write "a" (1100). bytes=1100.
        // "b": bytes >= 1024. push ("b", off_b). bytes=0.
        // write "b" (1100). bytes=1100.
        // "c": bytes >= 1024. push ("c", off_c).

        assert!(!index.is_empty());
        assert_eq!(index[0].0, "a");
        assert_eq!(index[0].1, 0);

        // Check seeking
        let mut iter = JSTableIterator::new(path.to_str().unwrap())?;
        // Seek to last index entry
        let last = index.last().unwrap();
        iter.seek(last.1)?;
        let (key, _) = iter.next().unwrap()?;
        assert_eq!(key, last.0);

        Ok(())
    }
}

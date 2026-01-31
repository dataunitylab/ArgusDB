use crate::schema::{InstanceType, Schema, SchemaExt};
use crate::{LazyDocument, SerdeWrapper, Value, make_static};
use jsonb_schema::{OwnedJsonb, RawJsonb};
use serde::{Deserialize, Serialize};
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
        let header_blob = jsonb_schema::to_owned_jsonb(&header)
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

            // Use SerdeWrapper to serialize jsonb Value via serde infrastructure
            let record = (id.clone(), SerdeWrapper(doc));
            let record_blob = jsonb_schema::to_owned_jsonb(&record)
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

pub struct JSTableLazyIterator {
    reader: BufReader<File>,
    pub timestamp: u64,
    pub collection: String,
    pub schema: Schema,
}

impl JSTableLazyIterator {
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

        let header_val = jsonb_schema::from_slice(&header_blob)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Convert jsonb_schema::Value -> String -> T
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

impl Iterator for JSTableLazyIterator {
    type Item = io::Result<LazyDocument>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(_) => {
                let record_len = u32::from_le_bytes(len_buf) as usize;
                let mut record_blob = vec![0u8; record_len];
                if let Err(e) = self.reader.read_exact(&mut record_blob) {
                    return Some(Err(e));
                }

                // Extract ID eagerly using RawJsonb to avoid full deserialization
                // record_blob is [id, doc]
                let id = {
                    let raw = RawJsonb::new(&record_blob);
                    if let Ok(Some(id_owned)) = raw.get_by_index(0) {
                        // id_owned is OwnedJsonb of the string.
                        // We need to decode just this string.
                        let id_bytes = id_owned.to_vec();
                        match jsonb_schema::from_slice(&id_bytes) {
                            Ok(jsonb_schema::Value::String(s)) => s.to_string(),
                            Ok(_) => {
                                return Some(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "ID is not a string",
                                )));
                            }
                            Err(e) => {
                                return Some(Err(io::Error::new(io::ErrorKind::InvalidData, e)));
                            }
                        }
                    } else {
                        return Some(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Failed to get ID from record",
                        )));
                    }
                };

                Some(Ok(LazyDocument {
                    id,
                    raw: record_blob,
                }))
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct JSTableIterator {
    inner: JSTableLazyIterator,
}

impl JSTableIterator {
    pub fn new(path: &str) -> io::Result<Self> {
        Ok(Self {
            inner: JSTableLazyIterator::new(path)?,
        })
    }

    pub fn seek(&mut self, offset: u64) -> io::Result<()> {
        self.inner.seek(offset)
    }

    // Accessors delegated to inner
    pub fn timestamp(&self) -> u64 {
        self.inner.timestamp
    }
    pub fn collection(&self) -> &str {
        &self.inner.collection
    }
    pub fn schema(&self) -> &Schema {
        &self.inner.schema
    }
}

impl Iterator for JSTableIterator {
    type Item = io::Result<(String, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(lazy_doc)) => {
                // Fully decode
                let val = match jsonb_schema::from_slice(&lazy_doc.raw)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
                {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };

                let static_val = make_static(&val);
                if let jsonb_schema::Value::Array(mut arr) = static_val
                    && arr.len() == 2
                {
                    let doc = arr.pop().unwrap(); // Last element is doc
                    // lazy_doc.id is already extracted
                    return Some(Ok((lazy_doc.id, doc)));
                }
                Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid record structure during materialization",
                )))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

pub fn read_jstable(path: &str) -> io::Result<JSTable> {
    let iterator = JSTableIterator::new(path)?;
    let timestamp = iterator.timestamp();
    let collection = iterator.collection().to_string();
    let schema = iterator.schema().clone();

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

    let mut merged_schema = Schema::new(InstanceType::Object);
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

    // Filter nulls (tombstones) - Value::Null matches jsonb Null
    use jsonb_schema::Value as JsonbValue;
    merged_documents.retain(|_, v| !matches!(v, JsonbValue::Null));

    JSTable::new(max_timestamp, collection, merged_schema, merged_documents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{InstanceType, SingleOrVec};
    use crate::{jsonb_to_serde, serde_to_jsonb};
    use serde_json::json;
    use tempfile::tempdir;
    use xorf::Filter;

    fn get_types(schema: &Schema) -> Vec<InstanceType> {
        match &schema.instance_type {
            Some(SingleOrVec::Single(t)) => vec![t.clone()],
            Some(SingleOrVec::Vec(v)) => v.clone(),
            None => vec![],
        }
    }

    #[test]
    fn test_read_jstable() -> Result<(), Box<dyn std::error::Error>> {
        let mut schema = Schema::new(InstanceType::Object);
        schema.properties = Some(BTreeMap::from([(
            "a".to_string(),
            Schema::new(InstanceType::Integer),
        )]));
        let mut documents = BTreeMap::new();
        documents.insert("id1".to_string(), serde_to_jsonb(json!({"a": 1})));
        documents.insert("id2".to_string(), serde_to_jsonb(json!({"a": 2})));
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
        assert_eq!(get_types(&read_table.schema), vec![InstanceType::Object]);
        assert_eq!(read_table.documents.len(), 2);
        // Compare values
        let v1 = read_table.documents.get("id1").unwrap();
        // convert to serde for easy comparison
        assert_eq!(jsonb_to_serde(v1), json!({"a": 1}));

        let v2 = read_table.documents.get("id2").unwrap();
        assert_eq!(jsonb_to_serde(v2), json!({"a": 2}));
        Ok(())
    }

    #[test]
    fn test_jstable_iterator() -> Result<(), Box<dyn std::error::Error>> {
        let mut schema = Schema::new(InstanceType::Object);
        schema.properties = Some(BTreeMap::from([(
            "a".to_string(),
            Schema::new(InstanceType::Integer),
        )]));
        let mut documents = BTreeMap::new();
        documents.insert("id1".to_string(), serde_to_jsonb(json!({"a": 1})));
        documents.insert("id2".to_string(), serde_to_jsonb(json!({"a": 2})));
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
        assert_eq!(iterator.timestamp(), 12345);
        assert_eq!(iterator.collection(), "test_col");

        let mut count = 0;
        let mut ids = Vec::new();
        for result in iterator {
            let (id, doc) = result?;
            count += 1;
            ids.push(id);
            let s_doc = jsonb_to_serde(&doc);
            assert!(s_doc == json!({"a": 1}) || s_doc == json!({"a": 2}));
        }
        assert_eq!(count, 2);
        assert!(ids.contains(&"id1".to_string()));
        assert!(ids.contains(&"id2".to_string()));

        Ok(())
    }

    #[test]
    fn test_read_filter() -> Result<(), Box<dyn std::error::Error>> {
        let mut schema = Schema::new(InstanceType::Object);
        schema.properties = Some(BTreeMap::from([(
            "a".to_string(),
            Schema::new(InstanceType::Integer),
        )]));
        let mut documents = BTreeMap::new();
        documents.insert("id1".to_string(), serde_to_jsonb(json!({"a": 1})));
        documents.insert("id2".to_string(), serde_to_jsonb(json!({"a": 2})));
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
        let schema = Schema::new(InstanceType::Object);

        let mut docs1 = BTreeMap::new();
        docs1.insert("id1".to_string(), serde_to_jsonb(json!({"v": 1})));
        let t1 = JSTable::new(100, "test_col".to_string(), schema.clone(), docs1);

        let mut docs2 = BTreeMap::new();
        docs2.insert("id1".to_string(), serde_to_jsonb(json!({"v": 2})));
        let t2 = JSTable::new(200, "test_col".to_string(), schema.clone(), docs2);

        // Case 1: t1 (older) then t2 (newer) in the slice
        let merged = merge_jstables(&[t1, t2]);
        assert_eq!(
            jsonb_to_serde(merged.documents.get("id1").unwrap()),
            json!({"v": 2})
        );
        assert_eq!(merged.timestamp, 200);
        assert_eq!(merged.collection, "test_col");

        // Case 2: Reverse order
        let mut docs1 = BTreeMap::new();
        docs1.insert("id1".to_string(), serde_to_jsonb(json!({"v": 1})));
        let t1b = JSTable::new(100, "test_col".to_string(), schema.clone(), docs1);

        let mut docs2 = BTreeMap::new();
        docs2.insert("id1".to_string(), serde_to_jsonb(json!({"v": 2})));
        let t2b = JSTable::new(200, "test_col".to_string(), schema.clone(), docs2);

        let merged_reverse = merge_jstables(&[t2b, t1b]);
        assert_eq!(
            jsonb_to_serde(merged_reverse.documents.get("id1").unwrap()),
            json!({"v": 2})
        );
        assert_eq!(merged_reverse.timestamp, 200);
    }

    #[test]
    fn test_jstable_keys_sorted_on_disk() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(InstanceType::Object);
        let mut documents = BTreeMap::new();
        // Insert keys in non-sorted order (BTreeMap will sort them)
        documents.insert("c".to_string(), serde_to_jsonb(json!(3)));
        documents.insert("a".to_string(), serde_to_jsonb(json!(1)));
        documents.insert("b".to_string(), serde_to_jsonb(json!(2)));

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
        let schema = Schema::new(InstanceType::Object);
        let mut documents = BTreeMap::new();

        let large_val = "x".repeat(500); // ~500 bytes
        documents.insert("a".to_string(), serde_to_jsonb(json!(large_val)));
        documents.insert("b".to_string(), serde_to_jsonb(json!(large_val)));
        documents.insert("c".to_string(), serde_to_jsonb(json!(large_val)));

        let larger_val = "x".repeat(1100);
        documents.insert("d".to_string(), serde_to_jsonb(json!(larger_val)));
        documents.insert("e".to_string(), serde_to_jsonb(json!(1)));

        let jstable = JSTable::new(123, "idx_test".to_string(), schema, documents);
        let dir = tempdir()?;
        let path = dir.path().join("idx_table");
        jstable.write(path.to_str().unwrap(), 1024)?;

        let index = read_index(path.to_str().unwrap())?;

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

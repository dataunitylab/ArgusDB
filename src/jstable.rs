use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};

pub struct JSTable {
    pub timestamp: u64,
    pub schema: Schema,
    pub documents: BTreeMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
struct JSTableHeader {
    timestamp: u64,
    schema: Schema,
}

impl JSTable {
    pub fn new(timestamp: u64, schema: Schema, documents: BTreeMap<String, Value>) -> Self {
        JSTable {
            timestamp,
            schema,
            documents,
        }
    }

    pub fn write(&self, path: &str) -> io::Result<()> {
        let mut file = File::create(path)?;

        // Write Header
        let header = JSTableHeader {
            timestamp: self.timestamp,
            schema: self.schema.clone(),
        };
        // Serialize header using jsonb
        let header_blob = jsonb::to_owned_jsonb(&header)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let header_bytes = header_blob.to_vec();
        let header_len = header_bytes.len() as u32;
        file.write_all(&header_len.to_le_bytes())?;
        file.write_all(&header_bytes)?;

        // Write Documents
        for (id, doc) in &self.documents {
            let record: (String, &Value) = (id.clone(), doc);
            let record_blob = jsonb::to_owned_jsonb(&record)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let record_bytes = record_blob.to_vec();
            let record_len = record_bytes.len() as u32;
            file.write_all(&record_len.to_le_bytes())?;
            file.write_all(&record_bytes)?;
        }
        Ok(())
    }
}

pub struct JSTableIterator {
    reader: BufReader<File>,
    pub timestamp: u64,
    pub schema: Schema,
}

impl JSTableIterator {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read Header Length
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let header_len = u32::from_le_bytes(len_buf) as usize;

        // Read Header Blob
        let mut header_blob = vec![0u8; header_len];
        reader.read_exact(&mut header_blob)?;

        let header_val = jsonb::from_slice(&header_blob)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Convert jsonb::Value -> String -> T
        let header_str = header_val.to_string();
        let header: JSTableHeader = serde_json::from_str(&header_str)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            reader,
            timestamp: header.timestamp,
            schema: header.schema,
        })
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
    let schema = iterator.schema.clone();

    let mut documents = BTreeMap::new();
    for result in iterator {
        let (id, doc) = result?;
        documents.insert(id, doc);
    }

    Ok(JSTable {
        timestamp,
        schema,
        documents,
    })
}

pub fn merge_jstables(tables: &[JSTable]) -> JSTable {
    let mut sorted_tables: Vec<&JSTable> = tables.iter().collect();
    sorted_tables.sort_by_key(|t| t.timestamp);

    let mut merged_schema = Schema::new(crate::schema::SchemaType::Object);
    let mut merged_documents = BTreeMap::new();
    let mut max_timestamp = 0;

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

    JSTable::new(max_timestamp, merged_schema, merged_documents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaType;
    use serde_json::json;
    use tempfile::NamedTempFile;

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
        let jstable = JSTable::new(12345, schema.clone(), documents.clone());

        let file = NamedTempFile::new().unwrap();
        jstable.write(file.path().to_str().unwrap()).unwrap();

        let read_table = read_jstable(file.path().to_str().unwrap()).unwrap();

        assert_eq!(read_table.timestamp, 12345);
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
        let jstable = JSTable::new(12345, schema.clone(), documents.clone());

        let file = NamedTempFile::new().unwrap();
        jstable.write(file.path().to_str().unwrap()).unwrap();

        let iterator = JSTableIterator::new(file.path().to_str().unwrap())?;
        assert_eq!(iterator.timestamp, 12345);

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
    fn test_merge_jstables_conflict_resolution() {
        let schema = Schema::new(SchemaType::Object);

        let mut docs1 = BTreeMap::new();
        docs1.insert("id1".to_string(), json!({"v": 1}));
        let t1 = JSTable::new(100, schema.clone(), docs1);

        let mut docs2 = BTreeMap::new();
        docs2.insert("id1".to_string(), json!({"v": 2}));
        let t2 = JSTable::new(200, schema.clone(), docs2);

        // Case 1: t1 (older) then t2 (newer) in the slice
        let merged = merge_jstables(&[t1, t2]);
        assert_eq!(*merged.documents.get("id1").unwrap(), json!({"v": 2}));
        assert_eq!(merged.timestamp, 200);

        // Case 2: Reverse order
        let mut docs1 = BTreeMap::new();
        docs1.insert("id1".to_string(), json!({"v": 1}));
        let t1b = JSTable::new(100, schema.clone(), docs1);

        let mut docs2 = BTreeMap::new();
        docs2.insert("id1".to_string(), json!({"v": 2}));
        let t2b = JSTable::new(200, schema.clone(), docs2);

        let merged_reverse = merge_jstables(&[t2b, t1b]);
        assert_eq!(
            *merged_reverse.documents.get("id1").unwrap(),
            json!({"v": 2})
        );
        assert_eq!(merged_reverse.timestamp, 200);
    }
}

use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{self, BufReader, BufRead, Write};
use std::fs::File;

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
        JSTable { timestamp, schema, documents }
    }

    pub fn write(&self, path: &str) -> io::Result<()> {
        let mut file = File::create(path)?;
        let header = JSTableHeader {
            timestamp: self.timestamp,
            schema: self.schema.clone(),
        };
        let header_json = serde_json::to_string(&header)?;
        writeln!(file, "{}", header_json)?;
        for (id, doc) in &self.documents {
            let record: (String, &Value) = (id.clone(), doc);
            let record_json = serde_json::to_string(&record)?;
            writeln!(file, "{}", record_json)?;
        }
        Ok(())
    }
}

pub fn read_jstable(path: &str) -> io::Result<JSTable> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header_line = lines.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing header line"))??;
    let header: JSTableHeader = serde_json::from_str(&header_line)?;

    let mut documents = BTreeMap::new();
    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        let record: (String, Value) = serde_json::from_str(&line)?;
        documents.insert(record.0, record.1);
    }

    Ok(JSTable { 
        timestamp: header.timestamp,
        schema: header.schema, 
        documents 
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
    use std::io::Write;

    #[test]
    fn test_read_jstable() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = NamedTempFile::new().unwrap();
        let schema = Schema {
            types: vec![SchemaType::Object],
            properties: Some(BTreeMap::from([
                ("a".to_string(), Schema::new(SchemaType::Integer)),
            ])),
            items: None,
        };
        let header = JSTableHeader {
            timestamp: 12345,
            schema: schema,
        };
        let header_json = serde_json::to_string(&header).unwrap();
        writeln!(file, "{}", header_json).unwrap();
        writeln!(file, "{}", serde_json::to_string(&json!(["id1", {"a": 1}])).unwrap()).unwrap();
        writeln!(file, "{}", serde_json::to_string(&json!(["id2", {"a": 2}])).unwrap()).unwrap();

        let jstable = read_jstable(file.path().to_str().unwrap()).unwrap();

        assert_eq!(jstable.timestamp, 12345);
        assert_eq!(jstable.schema.types, vec![SchemaType::Object]);
        assert_eq!(jstable.documents.len(), 2);
        assert_eq!(*jstable.documents.get("id1").unwrap(), json!({"a": 1}));
        assert_eq!(*jstable.documents.get("id2").unwrap(), json!({"a": 2}));
        Ok(())
    }

    #[test]
    fn test_write_jstable() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema {
            types: vec![SchemaType::Object],
            properties: Some(BTreeMap::from([
                ("a".to_string(), Schema::new(SchemaType::Integer)),
            ])),
            items: None,
        };
        let mut documents = BTreeMap::new();
        documents.insert("id1".to_string(), json!({"a": 1}));
        documents.insert("id2".to_string(), json!({"a": 2}));
        let jstable = JSTable::new(67890, schema, documents);

        let file = NamedTempFile::new().unwrap();
        jstable.write(file.path().to_str().unwrap()).unwrap();

        let content = std::fs::read_to_string(file.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        let header: JSTableHeader = serde_json::from_str(lines[0])?;
        
        assert_eq!(header.timestamp, 67890);
        assert_eq!(header.schema.types, vec![SchemaType::Object]);

        assert!(content.contains("[\"id1\",{\"a\":1}]"));
        assert!(content.contains("[\"id2\",{\"a\":2}]"));

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
        // Note: Creating array [t1, t2] moves them.
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
        assert_eq!(*merged_reverse.documents.get("id1").unwrap(), json!({"v": 2}));
        assert_eq!(merged_reverse.timestamp, 200);
    }
}

use crate::schema::Schema;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{self, BufReader, BufRead, Write};
use std::fs::File;

pub struct JSTable {
    pub schema: Schema,
    pub documents: BTreeMap<String, Value>,
}

impl JSTable {
    pub fn new(schema: Schema, documents: BTreeMap<String, Value>) -> Self {
        JSTable { schema, documents }
    }

    pub fn write(&self, path: &str) -> io::Result<()> {
        let mut file = File::create(path)?;
        let schema_json = serde_json::to_string(&self.schema)?;
        writeln!(file, "{}", schema_json)?;
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

    let schema_line = lines.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing schema line"))??;
    let schema: Schema = serde_json::from_str(&schema_line)?;

    let mut documents = BTreeMap::new();
    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        let record: (String, Value) = serde_json::from_str(&line)?;
        documents.insert(record.0, record.1);
    }

    Ok(JSTable { schema, documents })
}

pub fn merge_jstables(tables: &[JSTable]) -> JSTable {
    let mut merged_schema = Schema::new(crate::schema::SchemaType::Object);
    let mut merged_documents = BTreeMap::new();

    for table in tables {
        merged_schema.merge(table.schema.clone());
        for (id, doc) in &table.documents {
            merged_documents.insert(id.clone(), doc.clone());
        }
    }

    merged_documents.retain(|_, v| !v.is_null());

    JSTable::new(merged_schema, merged_documents)
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
        let schema_json = serde_json::to_string(&Schema {
            types: vec![SchemaType::Object],
            properties: Some(BTreeMap::from([
                ("a".to_string(), Schema::new(SchemaType::Integer)),
            ])),
            items: None,
        }).unwrap();
        writeln!(file, "{}", schema_json).unwrap();
        writeln!(file, "{}", serde_json::to_string(&json!(["id1", {"a": 1}])).unwrap()).unwrap();
        writeln!(file, "{}", serde_json::to_string(&json!(["id2", {"a": 2}])).unwrap()).unwrap();

        let jstable = read_jstable(file.path().to_str().unwrap()).unwrap();

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
        let jstable = JSTable::new(schema, documents);

        let file = NamedTempFile::new().unwrap();
        jstable.write(file.path().to_str().unwrap()).unwrap();

        let content = std::fs::read_to_string(file.path()).unwrap();
        assert!(content.contains("{\"type\":\"object\",\"properties\":{\"a\":{\"type\":[\"integer\"]}}}"));
        assert!(content.contains("[\"id1\",{\"a\":1}]"));
        assert!(content.contains("[\"id2\",{\"a\":2}]"));

        Ok(())
    }
}

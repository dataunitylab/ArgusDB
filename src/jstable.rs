use crate::schema::Schema;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{self, BufReader, BufRead};
use std::fs::File;

pub struct JSTable {
    pub schema: Schema,
    pub documents: BTreeMap<String, Value>,
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
        let record: (String, Value) = serde_json::from_str(&line)?;
        documents.insert(record.0, record.1);
    }

    Ok(JSTable { schema, documents })
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
        writeln!(file, "{}", serde_json::to_string(&json!(["id1", {"a": 1}]))?).unwrap();
        writeln!(file, "{}", serde_json::to_string(&json!(["id2", {"a": 2}]))?).unwrap();

        let jstable = read_jstable(file.path().to_str().unwrap()).unwrap();

        assert_eq!(jstable.schema.types, vec![SchemaType::Object]);
        assert_eq!(jstable.documents.len(), 2);
        assert_eq!(*jstable.documents.get("id1").unwrap(), json!({"a": 1}));
        assert_eq!(*jstable.documents.get("id2").unwrap(), json!({"a": 2}));
        Ok(())
    }
}

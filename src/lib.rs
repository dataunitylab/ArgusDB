pub mod bench_utils;
pub mod db;
pub mod expression;
pub mod jstable;
pub mod log;
pub mod parser;
pub mod query;
pub mod schema;
pub mod storage;

pub use expression::*;

use jsonb_schema::{Number, RawJsonb, Value as JsonbValue};
use serde::{Serialize, Serializer};

pub type Value = JsonbValue<'static>;

#[derive(Debug, Clone)]
pub struct LazyDocument {
    pub id: String,
    pub raw: Vec<u8>,
}

impl LazyDocument {
    pub fn is_tombstone(&self) -> bool {
        let raw = RawJsonb::new(&self.raw);
        if let Ok(Some(doc)) = raw.get_by_index(1) {
            doc.as_raw().is_null().unwrap_or(false)
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExecutionResult {
    Value(String, Value),
    Lazy(LazyDocument),
}

impl ExecutionResult {
    pub fn id(&self) -> &str {
        match self {
            ExecutionResult::Value(id, _) => id,
            ExecutionResult::Lazy(doc) => &doc.id,
        }
    }

    pub fn get_value(&self) -> Value {
        match self {
            ExecutionResult::Value(_, v) => v.clone(),
            ExecutionResult::Lazy(doc) => {
                // Decode the raw blob [id, document]
                if let Ok(val) = jsonb_schema::from_slice(&doc.raw) {
                    let static_val = make_static(&val);
                    if let JsonbValue::Array(mut arr) = static_val
                        && arr.len() == 2
                    {
                        return arr.pop().unwrap(); // doc
                    }
                }
                JsonbValue::Null
            }
        }
    }
}

// Public wrapper for serialization
pub struct SerdeWrapper<'a>(pub &'a Value);

impl<'a> Serialize for SerdeWrapper<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            JsonbValue::Null => serializer.serialize_unit(),
            JsonbValue::Bool(b) => serializer.serialize_bool(*b),
            JsonbValue::Number(n) => match n {
                Number::Int64(i) => serializer.serialize_i64(*i),
                Number::Float64(f) => serializer.serialize_f64(*f),
                Number::UInt64(u) => serializer.serialize_u64(*u),
                _ => serializer.serialize_unit(),
            },
            JsonbValue::String(s) => serializer.serialize_str(s),
            JsonbValue::Array(arr) => {
                use serde::ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                for e in arr {
                    seq.serialize_element(&SerdeWrapper(e))?;
                }
                seq.end()
            }
            JsonbValue::Object(obj) => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(obj.len()))?;
                for (k, v) in obj {
                    map.serialize_entry(k, &SerdeWrapper(v))?;
                }
                map.end()
            }
            _ => serializer.serialize_unit(),
        }
    }
}

pub mod serde_value {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(val: &Value, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let wrapper = SerdeWrapper(val);
        wrapper.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s_val = serde_json::Value::deserialize(deserializer)?;
        Ok(serde_to_jsonb(s_val))
    }
}

pub fn serde_to_jsonb(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => JsonbValue::Null,
        serde_json::Value::Bool(b) => JsonbValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                JsonbValue::Number(Number::Int64(i))
            } else if let Some(f) = n.as_f64() {
                JsonbValue::Number(Number::Float64(f))
            } else if let Some(u) = n.as_u64() {
                JsonbValue::Number(Number::UInt64(u))
            } else {
                JsonbValue::Null
            }
        }
        serde_json::Value::String(s) => JsonbValue::String(s.into()),
        serde_json::Value::Array(arr) => {
            let new_arr = arr.into_iter().map(serde_to_jsonb).collect();
            JsonbValue::Array(new_arr)
        }
        serde_json::Value::Object(obj) => JsonbValue::Object(
            obj.into_iter()
                .map(|(k, v)| (k, serde_to_jsonb(v)))
                .collect(),
        ),
    }
}

pub fn jsonb_to_serde(v: &Value) -> serde_json::Value {
    match v {
        JsonbValue::Null => serde_json::Value::Null,
        JsonbValue::Bool(b) => serde_json::Value::Bool(*b),
        JsonbValue::Number(n) => match n {
            Number::Int64(i) => serde_json::Value::Number((*i).into()),
            Number::Float64(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Number::UInt64(u) => serde_json::Value::Number((*u).into()),
            _ => serde_json::Value::Null,
        },
        JsonbValue::String(s) => serde_json::Value::String(s.to_string()),
        JsonbValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(jsonb_to_serde).collect())
        }
        JsonbValue::Object(obj) => {
            let mut new_obj = serde_json::Map::new();
            for (k, v) in obj {
                new_obj.insert(k.clone(), jsonb_to_serde(v));
            }
            serde_json::Value::Object(new_obj)
        }
        _ => serde_json::Value::Null,
    }
}

pub fn make_static(v: &JsonbValue) -> Value {
    match v {
        JsonbValue::Null => JsonbValue::Null,
        JsonbValue::Bool(b) => JsonbValue::Bool(*b),
        JsonbValue::Number(n) => JsonbValue::Number(n.clone()),
        JsonbValue::String(s) => JsonbValue::String(s.to_string().into()),
        JsonbValue::Array(arr) => JsonbValue::Array(arr.iter().map(make_static).collect()),
        JsonbValue::Object(obj) => JsonbValue::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), make_static(v)))
                .collect(),
        ),
        _ => JsonbValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lazy_document_is_tombstone() {
        let id = "test_id".to_string();

        // Case 1: Null document (Tombstone)
        let doc_null = crate::Value::Null;
        let record_null = (id.clone(), crate::SerdeWrapper(&doc_null));
        let blob_null = jsonb_schema::to_owned_jsonb(&record_null).unwrap();

        let lazy_null = LazyDocument {
            id: id.clone(),
            raw: blob_null.to_vec(),
        };
        assert!(lazy_null.is_tombstone());

        // Case 2: Non-null document
        let doc_obj = serde_to_jsonb(serde_json::json!({"a": 1}));
        let record_obj = (id.clone(), crate::SerdeWrapper(&doc_obj));
        let blob_obj = jsonb_schema::to_owned_jsonb(&record_obj).unwrap();

        let lazy_obj = LazyDocument {
            id: id.clone(),
            raw: blob_obj.to_vec(),
        };
        assert!(!lazy_obj.is_tombstone());
    }
}

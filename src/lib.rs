pub mod bench_utils;
pub mod db;
pub mod jstable;
pub mod log;
pub mod parser;
pub mod query;
pub mod schema;
pub mod storage;

use jsonb_schema::{Number, Value as JsonbValue};
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;

pub type Value = JsonbValue<'static>;

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
        serde_json::Value::Object(obj) => {
            let mut new_obj = BTreeMap::new();
            for (k, v) in obj {
                new_obj.insert(k, serde_to_jsonb(v));
            }
            JsonbValue::Object(new_obj)
        }
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
        JsonbValue::Object(obj) => {
            let mut new_obj = BTreeMap::new();
            for (k, v) in obj {
                new_obj.insert(k.clone(), make_static(v));
            }
            JsonbValue::Object(new_obj)
        }
        _ => JsonbValue::Null,
    }
}

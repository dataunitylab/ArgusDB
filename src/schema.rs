use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum SchemaType {
    String,
    Integer,
    Number,
    Boolean,
    Null,
    Object,
    Array,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Schema {
    #[serde(rename = "type")]
    pub types: Vec<SchemaType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, Schema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<Schema>>,
}

impl Schema {
    pub fn new(schema_type: SchemaType) -> Self {
        Schema {
            types: vec![schema_type],
            properties: None,
            items: None,
        }
    }

    pub fn merge(&mut self, other: Self) {
        for t in other.types {
            if !self.types.contains(&t) {
                self.types.push(t);
            }
        }

        if let Some(other_props) = other.properties {
            let self_props = self.properties.get_or_insert_with(BTreeMap::new);
            for (key, other_schema) in other_props {
                if let Some(self_schema) = self_props.get_mut(&key) {
                    self_schema.merge(other_schema);
                } else {
                    self_props.insert(key, other_schema);
                }
            }
        }

        if let Some(other_items) = other.items {
            if let Some(self_items) = self.items.as_mut() {
                self_items.merge(*other_items);
            } else {
                self.items = Some(other_items);
            }
        }
    }
}

pub fn infer_schema(doc: &Value) -> Schema {
    match doc {
        Value::Null => Schema::new(SchemaType::Null),
        Value::Bool(_) => Schema::new(SchemaType::Boolean),
        Value::Number(n) => {
            if n.is_i64() {
                Schema::new(SchemaType::Integer)
            } else {
                Schema::new(SchemaType::Number)
            }
        }
        Value::String(_) => Schema::new(SchemaType::String),
        Value::Array(arr) => {
            let mut items_schema = if let Some(first) = arr.first() {
                infer_schema(first)
            } else {
                // Empty array, we can't infer the type.
                // We could represent this as a special "unknown" type,
                // but for now we'll just make it an empty schema.
                Schema {
                    types: vec![],
                    properties: None,
                    items: None,
                }
            };

            for item in arr.iter().skip(1) {
                items_schema.merge(infer_schema(item));
            }

            Schema {
                types: vec![SchemaType::Array],
                properties: None,
                items: Some(Box::new(items_schema)),
            }
        }
        Value::Object(obj) => {
            let mut properties = BTreeMap::new();
            for (key, value) in obj {
                properties.insert(key.clone(), infer_schema(value));
            }
            Schema {
                types: vec![SchemaType::Object],
                properties: Some(properties),
                items: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_infer_simple_object() {
        let doc = json!({
            "a": 1,
            "b": "hello"
        });
        let schema = infer_schema(&doc);
        assert_eq!(schema.types, vec![SchemaType::Object]);
        let props = schema.properties.unwrap();
        assert_eq!(props.get("a").unwrap().types, vec![SchemaType::Integer]);
        assert_eq!(props.get("b").unwrap().types, vec![SchemaType::String]);
    }

    #[test]
    fn test_infer_nested_object() {
        let doc = json!({
            "a": {
                "b": true
            }
        });
        let schema = infer_schema(&doc);
        assert_eq!(schema.types, vec![SchemaType::Object]);
        let props = schema.properties.unwrap();
        let a_schema = props.get("a").unwrap();
        assert_eq!(a_schema.types, vec![SchemaType::Object]);
        let a_props = a_schema.properties.as_ref().unwrap();
        assert_eq!(a_props.get("b").unwrap().types, vec![SchemaType::Boolean]);
    }

    #[test]
    fn test_infer_array() {
        let doc = json!([1, 2, 3]);
        let schema = infer_schema(&doc);
        assert_eq!(schema.types, vec![SchemaType::Array]);
        let items = schema.items.unwrap();
        assert_eq!(items.types, vec![SchemaType::Integer]);
    }

    #[test]
    fn test_infer_array_mixed_types() {
        let doc = json!([1, "hello"]);
        let schema = infer_schema(&doc);
        assert_eq!(schema.types, vec![SchemaType::Array]);
        let items = schema.items.unwrap();
        assert_eq!(items.types.len(), 2);
        assert!(items.types.contains(&SchemaType::Integer));
        assert!(items.types.contains(&SchemaType::String));
    }

    #[test]
    fn test_merge_schemas() {
        let mut schema1 = infer_schema(&json!({"a": 1, "b": "hello"}));
        let schema2 = infer_schema(&json!({"b": 2, "c": "world"}));
        schema1.merge(schema2);

        assert_eq!(schema1.types, vec![SchemaType::Object]);
        let props = schema1.properties.unwrap();
        assert_eq!(props.get("a").unwrap().types, vec![SchemaType::Integer]);
        let b_types = &props.get("b").unwrap().types;
        assert_eq!(b_types.len(), 2);
        assert!(b_types.contains(&SchemaType::String));
        assert!(b_types.contains(&SchemaType::Integer));
        assert_eq!(props.get("c").unwrap().types, vec![SchemaType::String]);
    }

    #[test]
    fn test_infer_array_of_objects() {
        let doc = json!([
            {"a": 1},
            {"b": "hello"}
        ]);
        let schema = infer_schema(&doc);
        assert_eq!(schema.types, vec![SchemaType::Array]);
        let items = schema.items.unwrap();
        assert_eq!(items.types, vec![SchemaType::Object]);
        let props = items.properties.unwrap();
        assert_eq!(props.get("a").unwrap().types, vec![SchemaType::Integer]);
        assert_eq!(props.get("b").unwrap().types, vec![SchemaType::String]);
    }
    
    #[test]
    fn test_schema_type_variants() {
        assert_eq!(serde_json::to_string(&SchemaType::String).unwrap(), r#""string""#);
        assert_eq!(serde_json::to_string(&SchemaType::Integer).unwrap(), r#""integer""#);
        assert_eq!(serde_json::to_string(&SchemaType::Number).unwrap(), r#""number""#);
        assert_eq!(serde_json::to_string(&SchemaType::Boolean).unwrap(), r#""boolean""#);
        assert_eq!(serde_json::to_string(&SchemaType::Null).unwrap(), r#""null""#);
        assert_eq!(serde_json::to_string(&SchemaType::Object).unwrap(), r#""object""#);
        assert_eq!(serde_json::to_string(&SchemaType::Array).unwrap(), r#""array""#);
    }
}

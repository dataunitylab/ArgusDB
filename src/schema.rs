use crate::Value;
pub use jsonb_schema::schema::{InstanceType, Schema, SingleOrVec};
use jsonb_schema::{Number, Value as JsonbValue};
use std::collections::BTreeMap;

pub trait SchemaExt {
    fn new(instance_type: InstanceType) -> Self;
    fn merge(&mut self, other: Self);
}

impl SchemaExt for Schema {
    fn new(instance_type: InstanceType) -> Self {
        Schema {
            instance_type: Some(SingleOrVec::Single(instance_type)),
            ..Default::default()
        }
    }

    fn merge(&mut self, other: Self) {
        // Merge instance_type
        if let Some(other_type) = other.instance_type {
            match &mut self.instance_type {
                Some(self_type) => {
                    let mut types = match self_type {
                        SingleOrVec::Single(t) => vec![t.clone()],
                        SingleOrVec::Vec(v) => v.clone(),
                    };
                    let other_types = match other_type {
                        SingleOrVec::Single(t) => vec![t],
                        SingleOrVec::Vec(v) => v,
                    };

                    for t in other_types {
                        if !types.contains(&t) {
                            types.push(t);
                        }
                    }

                    if types.len() == 1 {
                        *self_type = SingleOrVec::Single(types[0].clone());
                    } else {
                        *self_type = SingleOrVec::Vec(types);
                    }
                }
                None => {
                    self.instance_type = Some(other_type);
                }
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
        JsonbValue::Null => Schema::new(InstanceType::Null),
        JsonbValue::Bool(_) => Schema::new(InstanceType::Boolean),
        JsonbValue::Number(n) => match n {
            Number::Int64(_) | Number::UInt64(_) => Schema::new(InstanceType::Integer),
            Number::Float64(_) => Schema::new(InstanceType::Number),
            _ => Schema::new(InstanceType::Number),
        },
        JsonbValue::String(_) => Schema::new(InstanceType::String),
        JsonbValue::Array(arr) => {
            let mut items_schema = if let Some(first) = arr.first() {
                infer_schema(first)
            } else {
                Schema::default()
            };

            for item in arr.iter().skip(1) {
                items_schema.merge(infer_schema(item));
            }

            let mut schema = Schema::new(InstanceType::Array);
            schema.items = Some(Box::new(items_schema));
            schema
        }
        JsonbValue::Object(obj) => {
            let mut properties = BTreeMap::new();
            for (key, value) in obj {
                properties.insert(key.clone(), infer_schema(value));
            }
            let mut schema = Schema::new(InstanceType::Object);
            schema.properties = Some(properties);
            schema
        }
        _ => Schema::new(InstanceType::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde_to_jsonb;
    use serde_json::json;

    fn get_types(schema: &Schema) -> Vec<InstanceType> {
        match &schema.instance_type {
            Some(SingleOrVec::Single(t)) => vec![t.clone()],
            Some(SingleOrVec::Vec(v)) => v.clone(),
            None => vec![],
        }
    }

    #[test]
    fn test_infer_simple_object() {
        let doc = serde_to_jsonb(json!({
            "a": 1,
            "b": "hello"
        }));
        let schema = infer_schema(&doc);
        assert_eq!(get_types(&schema), vec![InstanceType::Object]);
        let props = schema.properties.as_ref().unwrap();
        assert_eq!(
            get_types(props.get("a").unwrap()),
            vec![InstanceType::Integer]
        );
        assert_eq!(
            get_types(props.get("b").unwrap()),
            vec![InstanceType::String]
        );
    }

    #[test]
    fn test_infer_nested_object() {
        let doc = serde_to_jsonb(json!({
            "a": {
                "b": true
            }
        }));
        let schema = infer_schema(&doc);
        assert_eq!(get_types(&schema), vec![InstanceType::Object]);
        let props = schema.properties.as_ref().unwrap();
        let a_schema = props.get("a").unwrap();
        assert_eq!(get_types(a_schema), vec![InstanceType::Object]);
        let a_props = a_schema.properties.as_ref().unwrap();
        assert_eq!(
            get_types(a_props.get("b").unwrap()),
            vec![InstanceType::Boolean]
        );
    }

    #[test]
    fn test_infer_array() {
        let doc = serde_to_jsonb(json!([1, 2, 3]));
        let schema = infer_schema(&doc);
        assert_eq!(get_types(&schema), vec![InstanceType::Array]);
        let items = schema.items.as_ref().unwrap();
        assert_eq!(get_types(&items), vec![InstanceType::Integer]);
    }

    #[test]
    fn test_infer_array_mixed_types() {
        let doc = serde_to_jsonb(json!([1, "hello"]));
        let schema = infer_schema(&doc);
        assert_eq!(get_types(&schema), vec![InstanceType::Array]);
        let items = schema.items.as_ref().unwrap();
        let types = get_types(&items);
        assert_eq!(types.len(), 2);
        assert!(types.contains(&InstanceType::Integer));
        assert!(types.contains(&InstanceType::String));
    }

    #[test]
    fn test_merge_schemas() {
        let mut schema1 = infer_schema(&serde_to_jsonb(json!({"a": 1, "b": "hello"})));
        let schema2 = infer_schema(&serde_to_jsonb(json!({"b": 2, "c": "world"})));
        schema1.merge(schema2);

        assert_eq!(get_types(&schema1), vec![InstanceType::Object]);
        let props = schema1.properties.as_ref().unwrap();
        assert_eq!(
            get_types(props.get("a").unwrap()),
            vec![InstanceType::Integer]
        );
        let b_types = get_types(props.get("b").unwrap());
        assert_eq!(b_types.len(), 2);
        assert!(b_types.contains(&InstanceType::String));
        assert!(b_types.contains(&InstanceType::Integer));
        assert_eq!(
            get_types(props.get("c").unwrap()),
            vec![InstanceType::String]
        );
    }

    #[test]
    fn test_infer_array_of_objects() {
        let doc = serde_to_jsonb(json!([
            {"a": 1},
            {"b": "hello"}
        ]));
        let schema = infer_schema(&doc);
        assert_eq!(get_types(&schema), vec![InstanceType::Array]);
        let items = schema.items.as_ref().unwrap();
        assert_eq!(get_types(&items), vec![InstanceType::Object]);
        let props = items.properties.as_ref().unwrap();
        assert_eq!(
            get_types(props.get("a").unwrap()),
            vec![InstanceType::Integer]
        );
        assert_eq!(
            get_types(props.get("b").unwrap()),
            vec![InstanceType::String]
        );
    }

    #[test]
    fn test_schema_type_variants() {
        assert_eq!(
            serde_json::to_string(&InstanceType::String).unwrap(),
            r#""string""#
        );
        assert_eq!(
            serde_json::to_string(&InstanceType::Integer).unwrap(),
            r#""integer""#
        );
        assert_eq!(
            serde_json::to_string(&InstanceType::Number).unwrap(),
            r#""number""#
        );
        assert_eq!(
            serde_json::to_string(&InstanceType::Boolean).unwrap(),
            r#""boolean""#
        );
        assert_eq!(
            serde_json::to_string(&InstanceType::Null).unwrap(),
            r#""null""#
        );
        assert_eq!(
            serde_json::to_string(&InstanceType::Object).unwrap(),
            r#""object""#
        );
        assert_eq!(
            serde_json::to_string(&InstanceType::Array).unwrap(),
            r#""array""#
        );
    }
}

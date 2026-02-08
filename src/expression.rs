use crate::{LazyDocument, SerdeWrapper, Value, make_static};
use jsonb_schema::jsonpath::JsonPath;
use jsonb_schema::{Number, OwnedJsonb, RawJsonb};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum Expression<'a> {
    FieldReference(Vec<&'a str>, &'a str), // (split path in arena, raw string in arena)
    JsonPath(Box<JsonPath<'a>>, &'a str),  // (compiled path, raw string in arena)
    Literal(Value),
    Binary {
        left: Box<Expression<'a>>,
        op: BinaryOperator,
        right: Box<Expression<'a>>,
    },
    Logical {
        left: Box<Expression<'a>>,
        op: LogicalOperator,
        right: Box<Expression<'a>>,
    },
    Function {
        func: ScalarFunction,
        args: Vec<Expression<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarFunction {
    Abs,
    Acos,
    Acosh,
    Asin,
    Atan,
    Atan2,
    Ceil,
    Cos,
    Cosh,
    Div,
    Exp,
    Floor,
    Ln,
    Log,
    Log10,
    Pow,
    Rand,
    Round,
    Sign,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,
}

// Lazy Evaluator

pub fn evaluate_to_f64_lazy<'a>(expr: &Expression<'a>, doc: &LazyDocument) -> Option<f64> {
    match expr {
        Expression::FieldReference(parts, _) => {
            let raw_root = RawJsonb::new(&doc.raw);
            if let Ok(Some(doc_owned)) = raw_root.get_by_index(1) {
                if let Some(field_bytes) = get_path_lazy(doc_owned, parts) {
                    if let Ok(val) = jsonb_schema::from_slice(&field_bytes) {
                        match val {
                            jsonb_schema::Value::Number(n) => get_f64_from_number(&n),
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::Literal(val) => match val {
            Value::Number(n) => get_f64_from_number(n),
            _ => None,
        },
        _ => match evaluate_expression_lazy(expr, doc) {
            Value::Number(n) => get_f64_from_number(&n),
            _ => None,
        },
    }
}

pub fn evaluate_expression_lazy<'a>(expr: &Expression<'a>, doc: &LazyDocument) -> Value {
    match expr {
        Expression::FieldReference(parts, _) => {
            // Lazy optimization: only extract the requested field using RawJsonb
            // doc.raw is [id, document]
            let raw_root = RawJsonb::new(&doc.raw);
            // Get the document part (index 1)
            // Note: RawJsonb::get_by_index returns Result<Option<OwnedJsonb>>
            if let Ok(Some(doc_owned)) = raw_root.get_by_index(1)
                && let Some(field_bytes) = get_path_lazy(doc_owned, parts)
            {
                // Decode only the found field
                if let Ok(val) = jsonb_schema::from_slice(&field_bytes) {
                    return make_static(&val);
                }
            }
            Value::Null
        }
        Expression::JsonPath(json_path, _) => {
            // For JSON path, we still need to partially decode or at least traverse.
            // But jsonb_schema might not support running path on raw bytes easily without wrapper?
            // Actually, the previous code in evaluate_expression used:
            // let wrapper = SerdeWrapper(doc); to_owned_jsonb(&wrapper).as_raw().select_by_path(...)
            // Here we have bytes.
            let raw_root = RawJsonb::new(&doc.raw);
            if let Ok(Some(doc_owned)) = raw_root.get_by_index(1) {
                if let Ok(results) = doc_owned.as_raw().select_by_path(json_path) {
                    if results.is_empty() {
                        Value::Null
                    } else if results.len() == 1 {
                        let owned = results.into_iter().next().unwrap();
                        if let Ok(val) = jsonb_schema::from_slice(&owned.to_vec()) {
                            make_static(&val)
                        } else {
                            Value::Null
                        }
                    } else {
                        let mut arr = Vec::new();
                        for owned in results {
                            if let Ok(val) = jsonb_schema::from_slice(&owned.to_vec()) {
                                arr.push(make_static(&val));
                            }
                        }
                        Value::Array(arr)
                    }
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        Expression::Literal(val) => val.clone(),
        Expression::Binary { left, op, right } => {
            let l_val = evaluate_expression_lazy(left, doc);
            let r_val = evaluate_expression_lazy(right, doc);
            evaluate_binary(&l_val, op, &r_val)
        }
        Expression::Logical { left, op, right } => {
            let l_val = evaluate_expression_lazy(left, doc);
            let r_val = evaluate_expression_lazy(right, doc);
            evaluate_logical(&l_val, op, &r_val)
        }
        Expression::Function { func, args } => {
            let vals: Vec<Value> = args
                .iter()
                .map(|arg| evaluate_expression_lazy(arg, doc))
                .collect();
            evaluate_function(func, &vals)
        }
    }
}

fn get_path_lazy(mut current: OwnedJsonb, parts: &[&str]) -> Option<Vec<u8>> {
    for part in parts {
        let raw = current.as_raw();
        match raw.get_by_name(part, false) {
            Ok(Some(next)) => {
                current = next;
            }
            _ => return None,
        }
    }
    Some(current.to_vec())
}

pub fn evaluate_expression<'a>(expr: &Expression<'a>, doc: &Value) -> Value {
    match expr {
        Expression::FieldReference(parts, _) => get_path(doc, parts).unwrap_or(Value::Null),
        Expression::JsonPath(json_path, _) => {
            let wrapper = SerdeWrapper(doc);
            if let Ok(blob) = jsonb_schema::to_owned_jsonb(&wrapper) {
                // Execute select_by_path on RawJsonb
                if let Ok(results) = blob.as_raw().select_by_path(json_path) {
                    if results.is_empty() {
                        Value::Null
                    } else if results.len() == 1 {
                        // Extract single value
                        let owned = results.into_iter().next().unwrap();
                        let vec = owned.to_vec();
                        if let Ok(val) = jsonb_schema::from_slice(&vec) {
                            make_static(&val)
                        } else {
                            Value::Null
                        }
                    } else {
                        // Array of values
                        let mut arr = Vec::new();
                        for owned in results {
                            let vec = owned.to_vec();
                            if let Ok(val) = jsonb_schema::from_slice(&vec) {
                                arr.push(make_static(&val));
                            }
                        }
                        Value::Array(arr)
                    }
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        Expression::Literal(val) => val.clone(),
        Expression::Binary { left, op, right } => {
            let l_val = evaluate_expression(left, doc);
            let r_val = evaluate_expression(right, doc);
            evaluate_binary(&l_val, op, &r_val)
        }
        Expression::Logical { left, op, right } => {
            let l_val = evaluate_expression(left, doc);
            let r_val = evaluate_expression(right, doc);
            evaluate_logical(&l_val, op, &r_val)
        }
        Expression::Function { func, args } => {
            let vals: Vec<Value> = args
                .iter()
                .map(|arg| evaluate_expression(arg, doc))
                .collect();
            evaluate_function(func, &vals)
        }
    }
}

pub fn get_f64_from_number(n: &Number) -> Option<f64> {
    match n {
        Number::Int64(i) => Some(*i as f64),
        Number::UInt64(u) => Some(*u as f64),
        Number::Float64(f) => Some(*f),
        _ => None,
    }
}

fn get_i64_from_number(n: &Number) -> Option<i64> {
    match n {
        Number::Int64(i) => Some(*i),
        Number::UInt64(u) => i64::try_from(*u).ok(),
        _ => None,
    }
}

fn evaluate_function(func: &ScalarFunction, vals: &[Value]) -> Value {
    let get_f64 = |v: &Value| -> Option<f64> {
        match v {
            Value::Number(n) => get_f64_from_number(n),
            _ => None,
        }
    };

    let f1 = if !vals.is_empty() {
        get_f64(&vals[0])
    } else {
        None
    };

    let result = match func {
        // Unary
        ScalarFunction::Abs => f1.map(|f| f.abs()),
        ScalarFunction::Acos => f1.map(|f| f.acos()),
        ScalarFunction::Acosh => f1.map(|f| f.acosh()),
        ScalarFunction::Asin => f1.map(|f| f.asin()),
        ScalarFunction::Atan => f1.map(|f| f.atan()),
        ScalarFunction::Ceil => f1.map(|f| f.ceil()),
        ScalarFunction::Cos => f1.map(|f| f.cos()),
        ScalarFunction::Cosh => f1.map(|f| f.cosh()),
        ScalarFunction::Exp => f1.map(|f| f.exp()),
        ScalarFunction::Floor => f1.map(|f| f.floor()),
        ScalarFunction::Ln => f1.map(|f| f.ln()),
        ScalarFunction::Log10 => f1.map(|f| f.log10()),
        ScalarFunction::Sin => f1.map(|f| f.sin()),
        ScalarFunction::Sinh => f1.map(|f| f.sinh()),
        ScalarFunction::Sqrt => f1.map(|f| f.sqrt()),
        ScalarFunction::Tan => f1.map(|f| f.tan()),
        ScalarFunction::Tanh => f1.map(|f| f.tanh()),
        ScalarFunction::Sign => f1.map(|f| if f == 0.0 { 0.0 } else { f.signum() }),
        ScalarFunction::Rand => Some(rand::random::<f64>()),

        // Binary / Variable
        ScalarFunction::Atan2 => {
            let f2 = if vals.len() > 1 {
                get_f64(&vals[1])
            } else {
                None
            };
            match (f1, f2) {
                (Some(y), Some(x)) => Some(y.atan2(x)),
                _ => None,
            }
        }
        ScalarFunction::Div => {
            let f2 = if vals.len() > 1 {
                get_f64(&vals[1])
            } else {
                None
            };
            match (f1, f2) {
                (Some(x), Some(y)) => {
                    if y == 0.0 {
                        None
                    } else {
                        Some((x / y).trunc())
                    }
                }
                _ => None,
            }
        }
        ScalarFunction::Log => match f1 {
            Some(x) => {
                if vals.len() > 1 {
                    get_f64(&vals[1]).map(|y| x.log(y))
                } else {
                    Some(x.ln())
                }
            }
            None => None,
        },
        ScalarFunction::Pow => {
            let f2 = if vals.len() > 1 {
                get_f64(&vals[1])
            } else {
                None
            };
            match (f1, f2) {
                (Some(x), Some(y)) => Some(x.powf(y)),
                _ => None,
            }
        }
        ScalarFunction::Round => match f1 {
            Some(x) => {
                let decimals = if vals.len() > 1 {
                    get_f64(&vals[1]).unwrap_or(0.0) as i32
                } else {
                    0
                };
                let factor = 10.0f64.powi(decimals);
                Some((x * factor).round() / factor)
            }
            None => None,
        },
    };

    if let Some(res) = result {
        if res.is_nan() || res.is_infinite() {
            Value::Null
        } else {
            Value::Number(Number::Float64(res))
        }
    } else {
        Value::Null
    }
}

fn get_path(doc: &Value, parts: &[&str]) -> Option<Value> {
    let mut current = doc;
    for part in parts {
        match current {
            Value::Object(map) => {
                current = map.get(*part)?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

fn evaluate_binary(left: &Value, op: &BinaryOperator, right: &Value) -> Value {
    match op {
        BinaryOperator::Eq => Value::Bool(left == right),
        BinaryOperator::Neq => Value::Bool(left != right),
        BinaryOperator::Lt => compare_values(left, right)
            .map(|o| Value::Bool(o == Ordering::Less))
            .unwrap_or(Value::Bool(false)),
        BinaryOperator::Lte => compare_values(left, right)
            .map(|o| Value::Bool(o != Ordering::Greater))
            .unwrap_or(Value::Bool(false)),
        BinaryOperator::Gt => compare_values(left, right)
            .map(|o| Value::Bool(o == Ordering::Greater))
            .unwrap_or(Value::Bool(false)),
        BinaryOperator::Gte => compare_values(left, right)
            .map(|o| Value::Bool(o != Ordering::Less))
            .unwrap_or(Value::Bool(false)),
    }
}

fn evaluate_logical(left: &Value, op: &LogicalOperator, right: &Value) -> Value {
    let l_bool = left.as_bool().unwrap_or(false);
    let r_bool = right.as_bool().unwrap_or(false);
    match op {
        LogicalOperator::And => Value::Bool(l_bool && r_bool),
        LogicalOperator::Or => Value::Bool(l_bool || r_bool),
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(n1), Value::Number(n2)) => {
            if let (Some(i1), Some(i2)) = (get_i64_from_number(n1), get_i64_from_number(n2)) {
                i1.partial_cmp(&i2)
            } else {
                let f1: f64 = get_f64_from_number(n1)?;
                let f2: f64 = get_f64_from_number(n2)?;
                f1.partial_cmp(&f2)
            }
        }
        (Value::String(s1), Value::String(s2)) => Some(s1.cmp(s2)),
        (Value::Bool(b1), Value::Bool(b2)) => Some(b1.cmp(b2)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde_to_jsonb;
    use jsonb_schema::Value as JsonbValue;
    use serde_json::json;

    fn make_field_ref(s: &str) -> Expression<'_> {
        Expression::FieldReference(s.split('.').collect(), s)
    }

    #[test]
    fn test_functions() {
        let doc = serde_to_jsonb(json!({
            "neg": -10.5,
            "pos": 100,
            "val": 0.5,
            "one": 1.0,
            "zero": 0.0,
            "two": 2.0,
            "e": std::f64::consts::E,
            "pi_half": std::f64::consts::FRAC_PI_2,
            "nan_trigger": -1.0,
            "null_val": null,
            "str_val": "not a number"
        }));

        // Helper to evaluate function on a list of fields
        let eval_args = |func: ScalarFunction, fields: Vec<&str>| {
            let args = fields.iter().map(|f| make_field_ref(f)).collect();
            let expr = Expression::Function { func, args };
            evaluate_expression(&expr, &doc)
        };

        // Helper for unary
        let eval = |func: ScalarFunction, field: &str| eval_args(func, vec![field]);

        // ABS
        assert_eq!(
            eval(ScalarFunction::Abs, "neg"),
            serde_to_jsonb(json!(10.5))
        );

        // CEIL
        assert_eq!(
            eval(ScalarFunction::Ceil, "neg"),
            serde_to_jsonb(json!(-10.0))
        );
        assert_eq!(
            eval(ScalarFunction::Ceil, "val"),
            serde_to_jsonb(json!(1.0))
        );

        // FLOOR
        assert_eq!(
            eval(ScalarFunction::Floor, "neg"),
            serde_to_jsonb(json!(-11.0))
        );
        assert_eq!(
            eval(ScalarFunction::Floor, "val"),
            serde_to_jsonb(json!(0.0))
        );

        // SQRT
        assert_eq!(
            eval(ScalarFunction::Sqrt, "pos"),
            serde_to_jsonb(json!(10.0))
        );
        assert_eq!(eval(ScalarFunction::Sqrt, "nan_trigger"), Value::Null);

        // LN
        let ln_e = eval(ScalarFunction::Ln, "e").as_f64().unwrap();
        assert!((ln_e - 1.0).abs() < 1e-10);
        assert_eq!(eval(ScalarFunction::Ln, "nan_trigger"), Value::Null);

        // SIN
        let sin_val = eval(ScalarFunction::Sin, "pi_half").as_f64().unwrap();
        assert!((sin_val - 1.0).abs() < 1e-10);

        // COS -> ACOS
        // ACOS(0.5)
        let acos_val = eval(ScalarFunction::Acos, "val").as_f64().unwrap();
        assert!((acos_val - 0.5f64.acos()).abs() < 1e-10);
        // ACOS(2.0) -> NaN -> Null (using pos=100)
        assert_eq!(eval(ScalarFunction::Acos, "pos"), Value::Null);

        // ASIN
        let asin_val = eval(ScalarFunction::Asin, "val").as_f64().unwrap();
        assert!((asin_val - 0.5f64.asin()).abs() < 1e-10);

        // ATAN
        let atan_val = eval(ScalarFunction::Atan, "one").as_f64().unwrap();
        assert!((atan_val - 1.0f64.atan()).abs() < 1e-10);

        // TAN
        let tan_val = eval(ScalarFunction::Tan, "zero").as_f64().unwrap();
        assert!((tan_val - 0.0).abs() < 1e-10);

        // SIGN
        assert_eq!(
            eval(ScalarFunction::Sign, "neg"),
            serde_to_jsonb(json!(-1.0))
        );
        assert_eq!(
            eval(ScalarFunction::Sign, "pos"),
            serde_to_jsonb(json!(1.0))
        );
        assert_eq!(
            eval(ScalarFunction::Sign, "zero"),
            serde_to_jsonb(json!(0.0))
        );

        // EXP
        let exp_one = eval(ScalarFunction::Exp, "one").as_f64().unwrap();
        assert!((exp_one - std::f64::consts::E).abs() < 1e-10);

        // LOG10
        assert_eq!(
            eval(ScalarFunction::Log10, "pos"),
            serde_to_jsonb(json!(2.0))
        );

        // Binary Functions

        // DIV(100, 2) = 50
        assert_eq!(
            eval_args(ScalarFunction::Div, vec!["pos", "two"]),
            serde_to_jsonb(json!(50.0))
        );

        // POW(100, 0.5) = 10
        assert_eq!(
            eval_args(ScalarFunction::Pow, vec!["pos", "val"]),
            serde_to_jsonb(json!(10.0))
        );

        // ATAN2(1, 1) -> pi/4
        // ATAN2(y, x) = atan(y/x).
        // args: [one, one]. atan(1/1) = atan(1) = pi/4
        let atan2_val = eval_args(ScalarFunction::Atan2, vec!["one", "one"])
            .as_f64()
            .unwrap();
        assert!((atan2_val - std::f64::consts::FRAC_PI_4).abs() < 1e-10);

        // ATAN2(1, 0) -> pi/2 (Y=1, X=0)
        let atan2_val_2 = eval_args(ScalarFunction::Atan2, vec!["one", "zero"])
            .as_f64()
            .unwrap();
        assert!((atan2_val_2 - std::f64::consts::FRAC_PI_2).abs() < 1e-10);

        // ROUND
        // ROUND(0.5) -> 1.0
        assert_eq!(
            eval(ScalarFunction::Round, "val"),
            serde_to_jsonb(json!(1.0))
        );
        // ROUND(10.5) -> 11
        // ROUND(-10.5) -> -11
        assert_eq!(
            eval(ScalarFunction::Round, "neg"),
            serde_to_jsonb(json!(-11.0))
        );

        // RAND() -> non-deterministic
        let r1 = eval_args(ScalarFunction::Rand, vec![]);
        // match r1 { Value::Number(_) => ... }
        if let JsonbValue::Number(n) = r1 {
            let val = match n {
                Number::Float64(f) => f,
                _ => 0.0,
            };
            assert!(val >= 0.0 && val < 1.0);
        } else {
            panic!("Expected number");
        }

        // Edge cases
        assert_eq!(eval(ScalarFunction::Abs, "null_val"), Value::Null);
        assert_eq!(eval(ScalarFunction::Abs, "str_val"), Value::Null);
        assert_eq!(eval(ScalarFunction::Abs, "missing"), Value::Null);
    }

    #[test]
    fn test_functions_with_constants() {
        let doc = serde_to_jsonb(json!({}));

        // ABS(-10)
        let expr = Expression::Function {
            func: ScalarFunction::Abs,
            args: vec![Expression::Literal(serde_to_jsonb(json!(-10)))],
        };
        let result = evaluate_expression(&expr, &doc);
        assert_eq!(result, serde_to_jsonb(json!(10.0))); // json!(-10) is i64, result is f64 (10.0)

        // POW(2, 3)
        let expr = Expression::Function {
            func: ScalarFunction::Pow,
            args: vec![
                Expression::Literal(serde_to_jsonb(json!(2))),
                Expression::Literal(serde_to_jsonb(json!(3))),
            ],
        };
        let result = evaluate_expression(&expr, &doc);
        assert_eq!(result, serde_to_jsonb(json!(8.0)));
    }

    #[test]
    fn test_evaluate_binary() {
        let doc = serde_to_jsonb(json!({"a": 10, "b": 20}));

        let expr = Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Lt,
            right: Box::new(Expression::FieldReference(vec!["b"], "b")),
        };
        assert_eq!(evaluate_expression(&expr, &doc), Value::Bool(true));

        let expr = Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Eq,
            right: Box::new(Expression::Literal(serde_to_jsonb(json!(10)))),
        };
        assert_eq!(evaluate_expression(&expr, &doc), Value::Bool(true));
    }

    #[test]
    fn test_evaluate_logical() {
        let doc = serde_to_jsonb(json!({"a": true, "b": false}));

        let expr = Expression::Logical {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: LogicalOperator::And,
            right: Box::new(Expression::FieldReference(vec!["b"], "b")),
        };
        assert_eq!(evaluate_expression(&expr, &doc), Value::Bool(false));

        let expr = Expression::Logical {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: LogicalOperator::Or,
            right: Box::new(Expression::FieldReference(vec!["b"], "b")),
        };
        assert_eq!(evaluate_expression(&expr, &doc), Value::Bool(true));
    }

    #[test]
    fn test_evaluate_lazy() {
        use crate::SerdeWrapper;

        fn create_lazy(id: &str, val: serde_json::Value) -> LazyDocument {
            let doc = serde_to_jsonb(val);
            let record = (id.to_string(), SerdeWrapper(&doc));
            let blob = jsonb_schema::to_owned_jsonb(&record).unwrap();
            LazyDocument {
                id: id.to_string(),
                raw: blob.to_vec(),
            }
        }

        let lazy = create_lazy("id1", json!({"a": 10, "b": 20}));

        // Field Reference
        let expr = Expression::FieldReference(vec!["a"], "a");
        let val = evaluate_expression_lazy(&expr, &lazy);
        assert_eq!(val, serde_to_jsonb(json!(10)));

        // Lazy optimization check (evaluate_to_f64_lazy)
        let f = evaluate_to_f64_lazy(&expr, &lazy);
        assert_eq!(f, Some(10.0));
    }

    #[test]
    fn test_evaluate_lazy_jsonpath() {
        use crate::SerdeWrapper;

        fn create_lazy(id: &str, val: serde_json::Value) -> LazyDocument {
            let doc = serde_to_jsonb(val);
            let record = (id.to_string(), SerdeWrapper(&doc));
            let blob = jsonb_schema::to_owned_jsonb(&record).unwrap();
            LazyDocument {
                id: id.to_string(),
                raw: blob.to_vec(),
            }
        }

        let lazy = create_lazy("id1", json!({"a": {"b": 42}}));

        // Create JsonPath expression $.a.b
        let path_str = "$.a.b";
        let parsed = jsonb_schema::jsonpath::parse_json_path(path_str.as_bytes()).unwrap();
        let expr = Expression::JsonPath(Box::new(parsed), path_str);

        let val = evaluate_expression_lazy(&expr, &lazy);
        assert_eq!(val, serde_to_jsonb(json!(42)));
    }

    #[test]
    fn test_evaluate_lazy_binary_extended() {
        use crate::SerdeWrapper;

        fn create_lazy(id: &str, val: serde_json::Value) -> LazyDocument {
            let doc = serde_to_jsonb(val);
            let record = (id.to_string(), SerdeWrapper(&doc));
            let blob = jsonb_schema::to_owned_jsonb(&record).unwrap();
            LazyDocument {
                id: id.to_string(),
                raw: blob.to_vec(),
            }
        }

        let lazy = create_lazy("id1", json!({"a": 10, "b": 20}));

        // Neq
        let expr = Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Neq,
            right: Box::new(Expression::FieldReference(vec!["b"], "b")),
        };
        assert_eq!(evaluate_expression_lazy(&expr, &lazy), Value::Bool(true));

        // Lte
        let expr = Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Lte,
            right: Box::new(Expression::FieldReference(vec!["b"], "b")),
        };
        assert_eq!(evaluate_expression_lazy(&expr, &lazy), Value::Bool(true));

        // Gte
        let expr = Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Gte,
            right: Box::new(Expression::FieldReference(vec!["b"], "b")),
        };
        assert_eq!(evaluate_expression_lazy(&expr, &lazy), Value::Bool(false));
    }

    #[test]
    fn test_evaluate_to_f64_lazy_literal() {
        use crate::SerdeWrapper;

        // Literal Number
        let expr = Expression::Literal(serde_to_jsonb(json!(10.5)));
        // doc is irrelevant for literal but needed for signature
        let doc_val = serde_to_jsonb(json!({}));
        let record = ("id".to_string(), SerdeWrapper(&doc_val));
        let blob = jsonb_schema::to_owned_jsonb(&record).unwrap();
        let lazy = LazyDocument {
            id: "id".to_string(),
            raw: blob.to_vec(),
        };

        let f = evaluate_to_f64_lazy(&expr, &lazy);
        assert_eq!(f, Some(10.5));
    }
}

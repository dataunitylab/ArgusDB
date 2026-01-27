use crate::db::DB;
use crate::{SerdeWrapper, Value, make_static};
use jsonb_schema::Number;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use tracing::{Level, span};

#[derive(Debug, Clone)]
pub enum Expression {
    FieldReference(String), // dot notation e.g. "a.b"
    JsonPath(String),       // JSONPath e.g. "$.a.b"
    Literal(Value),
    Binary {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    Logical {
        left: Box<Expression>,
        op: LogicalOperator,
        right: Box<Expression>,
    },
    Function {
        func: ScalarFunction,
        args: Vec<Expression>,
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

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        collection: String,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expression,
    },
    Project {
        input: Box<LogicalPlan>,
        projections: Vec<Expression>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
    },
    Offset {
        input: Box<LogicalPlan>,
        offset: usize,
    },
}

#[derive(Debug, Clone)]
pub enum Statement {
    Insert {
        collection: String,
        documents: Vec<Value>,
    },
    Select(LogicalPlan),
    CreateCollection {
        collection: String,
    },
    DropCollection {
        collection: String,
    },
    ShowCollections,
}

// Iterator implementations for operators

pub struct ScanOperator<'a> {
    iter: Box<dyn Iterator<Item = (String, Value)> + 'a>,
}

impl<'a> ScanOperator<'a> {
    pub fn new(iter: Box<dyn Iterator<Item = (String, Value)> + 'a>) -> Self {
        ScanOperator { iter }
    }
}

impl<'a> Iterator for ScanOperator<'a> {
    type Item = (String, Value);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct FilterOperator<'a> {
    child: Box<dyn Iterator<Item = (String, Value)> + 'a>,
    predicate: Expression,
}

impl<'a> FilterOperator<'a> {
    pub fn new(
        child: Box<dyn Iterator<Item = (String, Value)> + 'a>,
        predicate: Expression,
    ) -> Self {
        FilterOperator { child, predicate }
    }
}

impl<'a> Iterator for FilterOperator<'a> {
    type Item = (String, Value);
    fn next(&mut self) -> Option<Self::Item> {
        for (id, doc) in self.child.by_ref() {
            if evaluate_expression(&self.predicate, &doc) == Value::Bool(true) {
                return Some((id, doc));
            }
        }
        None
    }
}

pub struct ProjectOperator<'a> {
    child: Box<dyn Iterator<Item = (String, Value)> + 'a>,
    projections: Vec<Expression>,
}

impl<'a> ProjectOperator<'a> {
    pub fn new(
        child: Box<dyn Iterator<Item = (String, Value)> + 'a>,
        projections: Vec<Expression>,
    ) -> Self {
        ProjectOperator { child, projections }
    }
}

impl<'a> Iterator for ProjectOperator<'a> {
    type Item = (String, Value);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, doc)) = self.child.next() {
            let mut new_doc = BTreeMap::new();
            for expr in &self.projections {
                let value = evaluate_expression(expr, &doc);
                match expr {
                    Expression::FieldReference(path) => {
                        new_doc.insert(path.clone(), value);
                    }
                    Expression::JsonPath(path) => {
                        new_doc.insert(path.clone(), value);
                    }
                    _ => {
                        // Fallback/TODO: Handle computed columns alias
                    }
                }
            }
            return Some((id, Value::Object(new_doc)));
        }
        None
    }
}

pub struct LimitOperator<'a> {
    child: Box<dyn Iterator<Item = (String, Value)> + 'a>,
    limit: usize,
    count: usize,
}

impl<'a> LimitOperator<'a> {
    pub fn new(child: Box<dyn Iterator<Item = (String, Value)> + 'a>, limit: usize) -> Self {
        LimitOperator {
            child,
            limit,
            count: 0,
        }
    }
}

impl<'a> Iterator for LimitOperator<'a> {
    type Item = (String, Value);
    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.limit {
            return None;
        }
        let item = self.child.next();
        if item.is_some() {
            self.count += 1;
        }
        item
    }
}

pub struct OffsetOperator<'a> {
    child: Box<dyn Iterator<Item = (String, Value)> + 'a>,
    offset: usize,
    skipped: usize,
}

impl<'a> OffsetOperator<'a> {
    pub fn new(child: Box<dyn Iterator<Item = (String, Value)> + 'a>, offset: usize) -> Self {
        OffsetOperator {
            child,
            offset,
            skipped: 0,
        }
    }
}

impl<'a> Iterator for OffsetOperator<'a> {
    type Item = (String, Value);
    fn next(&mut self) -> Option<Self::Item> {
        while self.skipped < self.offset {
            self.child.next()?;
            self.skipped += 1;
        }
        self.child.next()
    }
}

// Evaluator

fn evaluate_expression(expr: &Expression, doc: &Value) -> Value {
    match expr {
        Expression::FieldReference(path) => get_path(doc, path).unwrap_or(Value::Null),
        Expression::JsonPath(path_str) => {
            let wrapper = SerdeWrapper(doc);
            if let Ok(blob) = jsonb_schema::to_owned_jsonb(&wrapper) {
                // Parse path
                if let Ok(json_path) = jsonb_schema::jsonpath::parse_json_path(path_str.as_bytes())
                {
                    // Execute select_by_path on RawJsonb
                    if let Ok(results) = blob.as_raw().select_by_path(&json_path) {
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

fn get_f64_from_number(n: &Number) -> Option<f64> {
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
                (Some(x), Some(y)) => Some(y.atan2(x)),
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

fn get_path(doc: &Value, path: &str) -> Option<Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = doc;
    for part in parts {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
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

pub fn execute_plan<'a>(
    plan: LogicalPlan,
    db: &'a DB,
) -> Result<Box<dyn Iterator<Item = (String, Value)> + 'a>, String> {
    let span = span!(Level::DEBUG, "plan", plan = ?plan);
    let _enter = span.enter();

    match plan {
        LogicalPlan::Scan { collection } => {
            let iter = db.scan(&collection)?;
            Ok(Box::new(ScanOperator::new(iter)))
        }
        LogicalPlan::Filter { input, predicate } => {
            let child = execute_plan(*input, db)?;
            Ok(Box::new(FilterOperator::new(child, predicate)))
        }
        LogicalPlan::Project { input, projections } => {
            let child = execute_plan(*input, db)?;
            Ok(Box::new(ProjectOperator::new(child, projections)))
        }
        LogicalPlan::Limit { input, limit } => {
            let child = execute_plan(*input, db)?;
            Ok(Box::new(LimitOperator::new(child, limit)))
        }
        LogicalPlan::Offset { input, offset } => {
            let child = execute_plan(*input, db)?;
            Ok(Box::new(OffsetOperator::new(child, offset)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde_to_jsonb;
    use jsonb_schema::Value as JsonbValue;
    use serde_json::json;

    #[test]
    fn test_scan() {
        let data = vec![
            ("1".to_string(), serde_to_jsonb(json!({"a": 1}))),
            ("2".to_string(), serde_to_jsonb(json!({"a": 2}))),
        ];
        let source_iter = Box::new(data.into_iter());
        let mut scan = ScanOperator::new(source_iter);

        assert_eq!(scan.next().unwrap().0, "1");
        assert_eq!(scan.next().unwrap().0, "2");
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_filter() {
        let data = vec![
            ("1".to_string(), serde_to_jsonb(json!({"a": 1, "b": "yes"}))),
            ("2".to_string(), serde_to_jsonb(json!({"a": 2, "b": "no"}))),
            ("3".to_string(), serde_to_jsonb(json!({"a": 3, "b": "yes"}))),
        ];
        let source = Box::new(data.into_iter());

        let predicate = Expression::Logical {
            left: Box::new(Expression::Binary {
                left: Box::new(Expression::FieldReference("a".to_string())),
                op: BinaryOperator::Gt,
                right: Box::new(Expression::Literal(serde_to_jsonb(json!(1)))),
            }),
            op: LogicalOperator::And,
            right: Box::new(Expression::Binary {
                left: Box::new(Expression::FieldReference("b".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::Literal(serde_to_jsonb(json!("yes")))),
            }),
        };

        let mut filter = FilterOperator::new(source, predicate);

        let (id, _) = filter.next().unwrap();
        assert_eq!(id, "3");
        assert!(filter.next().is_none());
    }

    #[test]
    fn test_jsonpath() {
        let data = vec![
            ("1".to_string(), serde_to_jsonb(json!({"a": {"b": 10}}))),
            ("2".to_string(), serde_to_jsonb(json!({"a": {"b": 20}}))),
        ];
        let source = Box::new(data.into_iter());

        // Filter: $.a.b > 15
        let predicate = Expression::Binary {
            left: Box::new(Expression::JsonPath("$.a.b".to_string())),
            op: BinaryOperator::Gt,
            right: Box::new(Expression::Literal(serde_to_jsonb(json!(15)))),
        };

        let mut filter = FilterOperator::new(source, predicate);

        let (id, _) = filter.next().unwrap();
        assert_eq!(id, "2");
        assert!(filter.next().is_none());
    }

    #[test]
    fn test_project() {
        let data = vec![(
            "1".to_string(),
            serde_to_jsonb(json!({"a": 1, "b": 2, "c": 3})),
        )];
        let source = Box::new(data.into_iter());

        let projections = vec![
            Expression::FieldReference("a".to_string()),
            Expression::FieldReference("c".to_string()),
        ];

        let mut project = ProjectOperator::new(source, projections);

        let (_, doc) = project.next().unwrap();
        // Check fields using helper since as_object returns BTreeMap
        if let JsonbValue::Object(obj) = doc {
            assert_eq!(obj.len(), 2);
            assert_eq!(obj.get("a").unwrap(), &serde_to_jsonb(json!(1)));
            assert_eq!(obj.get("c").unwrap(), &serde_to_jsonb(json!(3)));
            assert!(obj.get("b").is_none());
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_limit_offset() {
        let data = vec![
            ("1".to_string(), serde_to_jsonb(json!({"a": 1}))),
            ("2".to_string(), serde_to_jsonb(json!({"a": 2}))),
            ("3".to_string(), serde_to_jsonb(json!({"a": 3}))),
            ("4".to_string(), serde_to_jsonb(json!({"a": 4}))),
        ];
        let source = Box::new(data.into_iter());

        let offset_op = Box::new(OffsetOperator::new(source, 1));
        let mut limit_op = LimitOperator::new(offset_op, 2);

        assert_eq!(limit_op.next().unwrap().0, "2");
        assert_eq!(limit_op.next().unwrap().0, "3");
        assert!(limit_op.next().is_none());
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
            let args = fields
                .iter()
                .map(|f| Expression::FieldReference(f.to_string()))
                .collect();
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
        // ATAN2(x, y) = atan(y/x).
        // args: [one, one]. atan(1/1) = atan(1) = pi/4
        let atan2_val = eval_args(ScalarFunction::Atan2, vec!["one", "one"])
            .as_f64()
            .unwrap();
        assert!((atan2_val - std::f64::consts::FRAC_PI_4).abs() < 1e-10);

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
}

use crate::db::DB;
use jsonpath_rust::query::js_path_vals;
use serde_json::Value;
use std::cmp::Ordering;

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
            let mut new_doc = serde_json::Map::new();
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
        Expression::JsonPath(path) => {
            if let Ok(nodes) = js_path_vals(path, doc) {
                if nodes.is_empty() {
                    Value::Null
                } else if nodes.len() == 1 {
                    nodes[0].clone()
                } else {
                    Value::Array(nodes.into_iter().cloned().collect())
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

fn evaluate_function(func: &ScalarFunction, vals: &[Value]) -> Value {
    let get_f64 = |v: &Value| -> Option<f64> {
        match v {
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    Some(f)
                } else {
                    n.as_i64().map(|i| i as f64)
                }
            }
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
                    match get_f64(&vals[1]) {
                        Some(y) => Some(x.log(y)),
                        None => None,
                    }
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
            serde_json::Number::from_f64(res)
                .map(Value::Number)
                .unwrap_or(Value::Null)
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
            if n1.is_i64() && n2.is_i64() {
                n1.as_i64().unwrap().partial_cmp(&n2.as_i64().unwrap())
            } else if n1.is_f64() && n2.is_f64() {
                n1.as_f64().unwrap().partial_cmp(&n2.as_f64().unwrap())
            } else {
                let f1 = n1.as_f64()?;
                let f2 = n2.as_f64()?;
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
    use serde_json::json;

    // Helper to create a mock source
    // Since we now use standard Iterator, we can just use vec::IntoIter

    #[test]
    fn test_scan() {
        let data = vec![
            ("1".to_string(), json!({"a": 1})),
            ("2".to_string(), json!({"a": 2})),
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
            ("1".to_string(), json!({"a": 1, "b": "yes"})),
            ("2".to_string(), json!({"a": 2, "b": "no"})),
            ("3".to_string(), json!({"a": 3, "b": "yes"})),
        ];
        let source = Box::new(data.into_iter());

        let predicate = Expression::Logical {
            left: Box::new(Expression::Binary {
                left: Box::new(Expression::FieldReference("a".to_string())),
                op: BinaryOperator::Gt,
                right: Box::new(Expression::Literal(json!(1))),
            }),
            op: LogicalOperator::And,
            right: Box::new(Expression::Binary {
                left: Box::new(Expression::FieldReference("b".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::Literal(json!("yes"))),
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
            ("1".to_string(), json!({"a": {"b": 10}})),
            ("2".to_string(), json!({"a": {"b": 20}})),
        ];
        let source = Box::new(data.into_iter());

        // Filter: $.a.b > 15
        let predicate = Expression::Binary {
            left: Box::new(Expression::JsonPath("$.a.b".to_string())),
            op: BinaryOperator::Gt,
            right: Box::new(Expression::Literal(json!(15))),
        };

        let mut filter = FilterOperator::new(source, predicate);

        let (id, _) = filter.next().unwrap();
        assert_eq!(id, "2");
        assert!(filter.next().is_none());
    }

    #[test]
    fn test_project() {
        let data = vec![("1".to_string(), json!({"a": 1, "b": 2, "c": 3}))];
        let source = Box::new(data.into_iter());

        let projections = vec![
            Expression::FieldReference("a".to_string()),
            Expression::FieldReference("c".to_string()),
        ];

        let mut project = ProjectOperator::new(source, projections);

        let (_, doc) = project.next().unwrap();
        let obj = doc.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj.get("a").unwrap(), &json!(1));
        assert_eq!(obj.get("c").unwrap(), &json!(3));
        assert!(obj.get("b").is_none());
    }

    #[test]
    fn test_limit_offset() {
        let data = vec![
            ("1".to_string(), json!({"a": 1})),
            ("2".to_string(), json!({"a": 2})),
            ("3".to_string(), json!({"a": 3})),
            ("4".to_string(), json!({"a": 4})),
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
        let doc = json!({
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
        });

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
        assert_eq!(eval(ScalarFunction::Abs, "neg"), json!(10.5));

        // CEIL
        assert_eq!(eval(ScalarFunction::Ceil, "neg"), json!(-10.0));
        assert_eq!(eval(ScalarFunction::Ceil, "val"), json!(1.0));

        // FLOOR
        assert_eq!(eval(ScalarFunction::Floor, "neg"), json!(-11.0));
        assert_eq!(eval(ScalarFunction::Floor, "val"), json!(0.0));

        // SQRT
        assert_eq!(eval(ScalarFunction::Sqrt, "pos"), json!(10.0));
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
        assert_eq!(eval(ScalarFunction::Sign, "neg"), json!(-1.0));
        assert_eq!(eval(ScalarFunction::Sign, "pos"), json!(1.0));
        assert_eq!(eval(ScalarFunction::Sign, "zero"), json!(0.0));

        // EXP
        let exp_one = eval(ScalarFunction::Exp, "one").as_f64().unwrap();
        assert!((exp_one - std::f64::consts::E).abs() < 1e-10);

        // LOG10
        assert_eq!(eval(ScalarFunction::Log10, "pos"), json!(2.0));

        // Binary Functions

        // DIV(100, 2) = 50
        assert_eq!(
            eval_args(ScalarFunction::Div, vec!["pos", "two"]),
            json!(50.0)
        );

        // POW(100, 0.5) = 10
        assert_eq!(
            eval_args(ScalarFunction::Pow, vec!["pos", "val"]),
            json!(10.0)
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
        assert_eq!(eval(ScalarFunction::Round, "val"), json!(1.0));
        // ROUND(10.5) -> 11
        // ROUND(-10.5) -> -11
        assert_eq!(eval(ScalarFunction::Round, "neg"), json!(-11.0));

        // RAND() -> non-deterministic
        let r1 = eval_args(ScalarFunction::Rand, vec![]);
        assert!(r1.is_number());
        let val = r1.as_f64().unwrap();
        assert!(val >= 0.0 && val < 1.0);

        // Edge cases
        assert_eq!(eval(ScalarFunction::Abs, "null_val"), Value::Null);
        assert_eq!(eval(ScalarFunction::Abs, "str_val"), Value::Null);
        assert_eq!(eval(ScalarFunction::Abs, "missing"), Value::Null);
    }
}

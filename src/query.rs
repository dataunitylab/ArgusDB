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
        arg: Box<Expression>,
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
    Asin,
    Atan,
    Ceil,
    Floor,
    Ln,
    Sin,
    Tan,
    Sqrt,
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
        Expression::Function { func, arg } => {
            let val = evaluate_expression(arg, doc);
            evaluate_function(func, &val)
        }
    }
}

fn evaluate_function(func: &ScalarFunction, val: &Value) -> Value {
    let f = match val {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                f
            } else if let Some(i) = n.as_i64() {
                i as f64
            } else {
                return Value::Null;
            }
        }
        _ => return Value::Null,
    };

    let result = match func {
        ScalarFunction::Abs => f.abs(),
        ScalarFunction::Acos => f.acos(),
        ScalarFunction::Asin => f.asin(),
        ScalarFunction::Atan => f.atan(),
        ScalarFunction::Ceil => f.ceil(),
        ScalarFunction::Floor => f.floor(),
        ScalarFunction::Ln => f.ln(),
        ScalarFunction::Sin => f.sin(),
        ScalarFunction::Tan => f.tan(),
        ScalarFunction::Sqrt => f.sqrt(),
    };

    if result.is_nan() || result.is_infinite() {
        Value::Null
    } else {
        serde_json::Number::from_f64(result)
            .map(Value::Number)
            .unwrap_or(Value::Null)
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
        let doc = json!({"a": -10.5, "b": 100});

        // ABS(a)
        let expr = Expression::Function {
            func: ScalarFunction::Abs,
            arg: Box::new(Expression::FieldReference("a".to_string())),
        };
        let result = evaluate_expression(&expr, &doc);
        assert_eq!(result, json!(10.5));

        // SQRT(b)
        let expr = Expression::Function {
            func: ScalarFunction::Sqrt,
            arg: Box::new(Expression::FieldReference("b".to_string())),
        };
        let result = evaluate_expression(&expr, &doc);
        assert_eq!(result, json!(10.0));

        // CEIL(a)
        let expr = Expression::Function {
            func: ScalarFunction::Ceil,
            arg: Box::new(Expression::FieldReference("a".to_string())),
        };
        let result = evaluate_expression(&expr, &doc);
        assert_eq!(result, json!(-10.0));
    }
}

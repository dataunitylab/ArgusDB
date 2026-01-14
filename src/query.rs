use serde_json::Value;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum Expression {
    FieldReference(String), // dot notation e.g. "a.b"
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
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Eq, Neq, Lt, Lte, Gt, Gte
}

#[derive(Debug, Clone)]
pub enum LogicalOperator {
    And, Or
}

pub trait QueryOperator {
    fn next(&mut self) -> Option<(String, Value)>;
}

impl Iterator for Box<dyn QueryOperator> {
    type Item = (String, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.as_mut().next()
    }
}

pub struct ScanOperator<'a> {
    iter: Box<dyn Iterator<Item = (String, Value)> + 'a>,
}

impl<'a> ScanOperator<'a> {
    pub fn new(iter: Box<dyn Iterator<Item = (String, Value)> + 'a>) -> Self {
        ScanOperator { iter }
    }
}

impl<'a> QueryOperator for ScanOperator<'a> {
    fn next(&mut self) -> Option<(String, Value)> {
        self.iter.next()
    }
}

pub struct FilterOperator {
    child: Box<dyn QueryOperator>,
    predicate: Expression,
}

impl FilterOperator {
    pub fn new(child: Box<dyn QueryOperator>, predicate: Expression) -> Self {
        FilterOperator { child, predicate }
    }
}

impl QueryOperator for FilterOperator {
    fn next(&mut self) -> Option<(String, Value)> {
        while let Some((id, doc)) = self.child.next() {
            if evaluate_expression(&self.predicate, &doc) == Value::Bool(true) {
                return Some((id, doc));
            }
        }
        None
    }
}

pub struct ProjectOperator {
    child: Box<dyn QueryOperator>,
    projections: Vec<Expression>, 
}

impl ProjectOperator {
    pub fn new(child: Box<dyn QueryOperator>, projections: Vec<Expression>) -> Self {
        ProjectOperator { child, projections }
    }
}

impl QueryOperator for ProjectOperator {
    fn next(&mut self) -> Option<(String, Value)> {
        if let Some((id, doc)) = self.child.next() {
            let mut new_doc = serde_json::Map::new();
            for expr in &self.projections {
                let value = evaluate_expression(expr, &doc);
                match expr {
                    Expression::FieldReference(path) => {
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

pub struct LimitOperator {
    child: Box<dyn QueryOperator>,
    limit: usize,
    count: usize,
}

impl LimitOperator {
    pub fn new(child: Box<dyn QueryOperator>, limit: usize) -> Self {
        LimitOperator { child, limit, count: 0 }
    }
}

impl QueryOperator for LimitOperator {
    fn next(&mut self) -> Option<(String, Value)> {
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

pub struct OffsetOperator {
    child: Box<dyn QueryOperator>,
    offset: usize,
    skipped: usize,
}

impl OffsetOperator {
    pub fn new(child: Box<dyn QueryOperator>, offset: usize) -> Self {
        OffsetOperator { child, offset, skipped: 0 }
    }
}

impl QueryOperator for OffsetOperator {
    fn next(&mut self) -> Option<(String, Value)> {
        while self.skipped < self.offset {
            if self.child.next().is_none() {
                return None;
            }
            self.skipped += 1;
        }
        self.child.next()
    }
}

// Evaluator

fn evaluate_expression(expr: &Expression, doc: &Value) -> Value {
    match expr {
        Expression::FieldReference(path) => {
            get_path(doc, path).unwrap_or(Value::Null)
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
        BinaryOperator::Lt => compare_values(left, right).map(|o| Value::Bool(o == Ordering::Less)).unwrap_or(Value::Bool(false)),
        BinaryOperator::Lte => compare_values(left, right).map(|o| Value::Bool(o != Ordering::Greater)).unwrap_or(Value::Bool(false)),
        BinaryOperator::Gt => compare_values(left, right).map(|o| Value::Bool(o == Ordering::Greater)).unwrap_or(Value::Bool(false)),
        BinaryOperator::Gte => compare_values(left, right).map(|o| Value::Bool(o != Ordering::Less)).unwrap_or(Value::Bool(false)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct MockSource {
        data: std::vec::IntoIter<(String, Value)>,
    }

    impl MockSource {
        fn new(data: Vec<(String, Value)>) -> Self {
            MockSource { data: data.into_iter() }
        }
    }

    impl QueryOperator for MockSource {
        fn next(&mut self) -> Option<(String, Value)> {
            self.data.next()
        }
    }

    #[test]
    fn test_scan() {
        let data = vec![
            ("1".to_string(), json!({"a": 1})),
            ("2".to_string(), json!({"a": 2})),
        ];
        // ScanOperator wraps Iterator, MockSource wraps IntoIter
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
        let source = Box::new(MockSource::new(data));
        
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
    fn test_project() {
        let data = vec![
            ("1".to_string(), json!({"a": 1, "b": 2, "c": 3})),
        ];
        let source = Box::new(MockSource::new(data));
        
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
        let source = Box::new(MockSource::new(data));
        
        let offset_op = Box::new(OffsetOperator::new(source, 1));
        let mut limit_op = LimitOperator::new(offset_op, 2);
        
        assert_eq!(limit_op.next().unwrap().0, "2");
        assert_eq!(limit_op.next().unwrap().0, "3");
        assert!(limit_op.next().is_none());
    }
}

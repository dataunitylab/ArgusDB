use crate::db::DB;
pub use crate::expression::*;
use crate::{ExecutionResult, Value};
use std::collections::BTreeMap;
use tracing::{Level, span};

#[derive(Debug, Clone)]
pub enum LogicalPlan<'a> {
    Scan {
        collection: String, // Keep String for now to avoid arena requirement for simple scans if possible, but actually we will put it in arena for consistency
    },
    Filter {
        input: Box<LogicalPlan<'a>>,
        predicate: Expression<'a>,
    },
    Project {
        input: Box<LogicalPlan<'a>>,
        projections: Vec<Expression<'a>>,
    },
    Limit {
        input: Box<LogicalPlan<'a>>,
        limit: usize,
    },
    Offset {
        input: Box<LogicalPlan<'a>>,
        offset: usize,
    },
}

#[derive(Debug, Clone)]
pub enum Statement<'a> {
    Insert {
        collection: String,
        documents: Vec<Value>,
    },
    Select(LogicalPlan<'a>),
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
    iter: Box<dyn Iterator<Item = ExecutionResult> + 'a>,
}

impl<'a> ScanOperator<'a> {
    pub fn new(iter: Box<dyn Iterator<Item = ExecutionResult> + 'a>) -> Self {
        ScanOperator { iter }
    }
}

impl<'a> Iterator for ScanOperator<'a> {
    type Item = ExecutionResult;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct FilterOperator<'a> {
    child: Box<dyn Iterator<Item = ExecutionResult> + 'a>,
    predicate: Expression<'a>,
}

impl<'a> FilterOperator<'a> {
    pub fn new(
        child: Box<dyn Iterator<Item = ExecutionResult> + 'a>,
        predicate: Expression<'a>,
    ) -> Self {
        FilterOperator { child, predicate }
    }
}

impl<'a> Iterator for FilterOperator<'a> {
    type Item = ExecutionResult;
    fn next(&mut self) -> Option<Self::Item> {
        for item in self.child.by_ref() {
            let keep = match &item {
                ExecutionResult::Value(_, doc) => {
                    evaluate_expression(&self.predicate, doc) == Value::Bool(true)
                }
                ExecutionResult::Lazy(doc) => {
                    evaluate_expression_lazy(&self.predicate, doc) == Value::Bool(true)
                }
            };
            if keep {
                return Some(item);
            }
        }
        None
    }
}

pub struct ProjectOperator<'a> {
    child: Box<dyn Iterator<Item = ExecutionResult> + 'a>,
    projections: Vec<Expression<'a>>,
}

impl<'a> ProjectOperator<'a> {
    pub fn new(
        child: Box<dyn Iterator<Item = ExecutionResult> + 'a>,
        projections: Vec<Expression<'a>>,
    ) -> Self {
        ProjectOperator { child, projections }
    }
}

impl<'a> Iterator for ProjectOperator<'a> {
    type Item = ExecutionResult;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.child.next() {
            let id = item.id().to_string();
            let mut new_doc = BTreeMap::new();
            for expr in &self.projections {
                let value = match &item {
                    ExecutionResult::Value(_, doc) => evaluate_expression(expr, doc),
                    ExecutionResult::Lazy(doc) => evaluate_expression_lazy(expr, doc),
                };
                match expr {
                    Expression::FieldReference(_, raw) => {
                        new_doc.insert(raw.to_string(), value);
                    }
                    Expression::JsonPath(_, raw) => {
                        new_doc.insert(raw.to_string(), value);
                    }
                    _ => {
                        // Fallback/TODO: Handle computed columns alias
                    }
                }
            }
            return Some(ExecutionResult::Value(id, Value::Object(new_doc)));
        }
        None
    }
}

pub struct LimitOperator<'a> {
    child: Box<dyn Iterator<Item = ExecutionResult> + 'a>,
    limit: usize,
    count: usize,
}

impl<'a> LimitOperator<'a> {
    pub fn new(child: Box<dyn Iterator<Item = ExecutionResult> + 'a>, limit: usize) -> Self {
        LimitOperator {
            child,
            limit,
            count: 0,
        }
    }
}

impl<'a> Iterator for LimitOperator<'a> {
    type Item = ExecutionResult;
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
    child: Box<dyn Iterator<Item = ExecutionResult> + 'a>,
    offset: usize,
    skipped: usize,
}

impl<'a> OffsetOperator<'a> {
    pub fn new(child: Box<dyn Iterator<Item = ExecutionResult> + 'a>, offset: usize) -> Self {
        OffsetOperator {
            child,
            offset,
            skipped: 0,
        }
    }
}

impl<'a> Iterator for OffsetOperator<'a> {
    type Item = ExecutionResult;
    fn next(&mut self) -> Option<Self::Item> {
        while self.skipped < self.offset {
            self.child.next()?;
            self.skipped += 1;
        }
        self.child.next()
    }
}

// Evaluator

pub fn execute_plan<'a>(
    plan: LogicalPlan<'a>,
    db: &'a DB,
) -> Result<Box<dyn Iterator<Item = ExecutionResult> + 'a>, String> {
    let span = span!(Level::DEBUG, "plan", plan = ?plan);
    let _enter = span.enter();

    match plan {
        LogicalPlan::Scan { collection } => {
            let iter = db.scan(&collection, None, None)?;
            Ok(Box::new(ScanOperator::new(iter)))
        }
        LogicalPlan::Filter { input, predicate } => match *input {
            LogicalPlan::Scan { collection } => {
                let iter = db.scan(&collection, Some(predicate), None)?;
                Ok(Box::new(ScanOperator::new(iter)))
            }
            other_input => {
                let child = execute_plan(other_input, db)?;
                Ok(Box::new(FilterOperator::new(child, predicate)))
            }
        },
        LogicalPlan::Project { input, projections } => {
            match *input {
                LogicalPlan::Scan { collection } => {
                    // Pushdown project to Scan
                    let iter = db.scan(&collection, None, Some(projections))?;
                    Ok(Box::new(ScanOperator::new(iter)))
                }
                LogicalPlan::Filter {
                    input: inner,
                    predicate,
                } => {
                    match *inner {
                        LogicalPlan::Scan { collection } => {
                            // Pushdown project + filter to Scan
                            let iter = db.scan(&collection, Some(predicate), Some(projections))?;
                            Ok(Box::new(ScanOperator::new(iter)))
                        }
                        other_inner => {
                            // Standard execution
                            let input_node = LogicalPlan::Filter {
                                input: Box::new(other_inner),
                                predicate,
                            };
                            let child = execute_plan(input_node, db)?;
                            Ok(Box::new(ProjectOperator::new(child, projections)))
                        }
                    }
                }
                other_input => {
                    let child = execute_plan(other_input, db)?;
                    Ok(Box::new(ProjectOperator::new(child, projections)))
                }
            }
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

    fn make_field_ref(s: &str) -> Expression<'_> {
        Expression::FieldReference(s.split('.').collect(), s)
    }

    fn make_json_path(s: &str) -> Expression<'_> {
        Expression::JsonPath(
            Box::new(jsonb_schema::jsonpath::parse_json_path(s.as_bytes()).unwrap()),
            s,
        )
    }

    fn to_exec_result(id: &str, val: Value) -> ExecutionResult {
        ExecutionResult::Value(id.to_string(), val)
    }

    #[test]
    fn test_scan() {
        let data = vec![
            to_exec_result("1", serde_to_jsonb(json!({"a": 1}))),
            to_exec_result("2", serde_to_jsonb(json!({"a": 2}))),
        ];
        let source_iter = Box::new(data.into_iter());
        let mut scan = ScanOperator::new(source_iter);

        assert_eq!(scan.next().unwrap().id(), "1");
        assert_eq!(scan.next().unwrap().id(), "2");
        assert!(scan.next().is_none());
    }

    #[test]
    fn test_filter() {
        let data = vec![
            to_exec_result("1", serde_to_jsonb(json!({"a": 1, "b": "yes"}))),
            to_exec_result("2", serde_to_jsonb(json!({"a": 2, "b": "no"}))),
            to_exec_result("3", serde_to_jsonb(json!({"a": 3, "b": "yes"}))),
        ];
        let source = Box::new(data.into_iter());

        let predicate = Expression::Logical {
            left: Box::new(Expression::Binary {
                left: Box::new(make_field_ref("a")),
                op: BinaryOperator::Gt,
                right: Box::new(Expression::Literal(serde_to_jsonb(json!(1)))),
            }),
            op: LogicalOperator::And,
            right: Box::new(Expression::Binary {
                left: Box::new(make_field_ref("b")),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::Literal(serde_to_jsonb(json!("yes")))),
            }),
        };

        let mut filter = FilterOperator::new(source, predicate);

        let item = filter.next().unwrap();
        assert_eq!(item.id(), "3");
        assert!(filter.next().is_none());
    }

    #[test]
    fn test_jsonpath() {
        let data = vec![
            to_exec_result("1", serde_to_jsonb(json!({"a": {"b": 10}}))),
            to_exec_result("2", serde_to_jsonb(json!({"a": {"b": 20}}))),
        ];
        let source = Box::new(data.into_iter());

        // Filter: $.a.b > 15
        let predicate = Expression::Binary {
            left: Box::new(make_json_path("$.a.b")),
            op: BinaryOperator::Gt,
            right: Box::new(Expression::Literal(serde_to_jsonb(json!(15)))),
        };

        let mut filter = FilterOperator::new(source, predicate);

        let item = filter.next().unwrap();
        assert_eq!(item.id(), "2");
        assert!(filter.next().is_none());
    }

    #[test]
    fn test_project() {
        let data = vec![to_exec_result(
            "1",
            serde_to_jsonb(json!({"a": 1, "b": 2, "c": 3})),
        )];
        let source = Box::new(data.into_iter());

        let projections = vec![make_field_ref("a"), make_field_ref("c")];

        let mut project = ProjectOperator::new(source, projections);

        let item = project.next().unwrap();
        let doc = item.get_value();
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
            to_exec_result("1", serde_to_jsonb(json!({"a": 1}))),
            to_exec_result("2", serde_to_jsonb(json!({"a": 2}))),
            to_exec_result("3", serde_to_jsonb(json!({"a": 3}))),
            to_exec_result("4", serde_to_jsonb(json!({"a": 4}))),
        ];
        let source = Box::new(data.into_iter());

        let offset_op = Box::new(OffsetOperator::new(source, 1));
        let mut limit_op = LimitOperator::new(offset_op, 2);

        assert_eq!(limit_op.next().unwrap().id(), "2");
        assert_eq!(limit_op.next().unwrap().id(), "3");
        assert!(limit_op.next().is_none());
    }
}

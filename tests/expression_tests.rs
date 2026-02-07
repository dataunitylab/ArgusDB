use argusdb::expression::{
    BinaryOperator, Expression, ScalarFunction, evaluate_expression, evaluate_to_f64_lazy,
};
use argusdb::parser::parse;
use argusdb::query::{LogicalPlan, Statement};
use argusdb::{LazyDocument, SerdeWrapper, Value, serde_to_jsonb};
use bumpalo::Bump;
use serde_json::json;

#[test]
fn test_evaluate_jsonpath_via_sql() {
    let sql = "SELECT $.a.b FROM t";
    let arena = Bump::new();
    let stmt = parse(sql, &arena).unwrap();

    let expr = match stmt {
        Statement::Select(LogicalPlan::Project { projections, .. }) => projections[0].clone(),
        _ => panic!("Unexpected plan"),
    };

    let doc = serde_to_jsonb(json!({"a": {"b": 42}}));
    let result = evaluate_expression(&expr, &doc);
    assert_eq!(result, serde_to_jsonb(json!(42)));
}

#[test]
fn test_evaluate_binary_ops_extended() {
    let doc = serde_to_jsonb(json!({"a": 10}));

    // Neq
    let expr = Expression::Binary {
        left: Box::new(Expression::FieldReference(vec!["a"], "a")),
        op: BinaryOperator::Neq,
        right: Box::new(Expression::Literal(serde_to_jsonb(json!(5)))),
    };
    assert_eq!(evaluate_expression(&expr, &doc), Value::Bool(true));

    // Lte
    let expr = Expression::Binary {
        left: Box::new(Expression::FieldReference(vec!["a"], "a")),
        op: BinaryOperator::Lte,
        right: Box::new(Expression::Literal(serde_to_jsonb(json!(10)))),
    };
    assert_eq!(evaluate_expression(&expr, &doc), Value::Bool(true));

    // Gte
    let expr = Expression::Binary {
        left: Box::new(Expression::FieldReference(vec!["a"], "a")),
        op: BinaryOperator::Gte,
        right: Box::new(Expression::Literal(serde_to_jsonb(json!(10)))),
    };
    assert_eq!(evaluate_expression(&expr, &doc), Value::Bool(true));
}

#[test]
fn test_evaluate_lazy_literal() {
    let id = "id".to_string();
    let doc_val = serde_to_jsonb(json!({"a": 1}));
    let record = (id.clone(), SerdeWrapper(&doc_val));
    let blob = jsonb_schema::to_owned_jsonb(&record).unwrap();
    let lazy = LazyDocument {
        id,
        raw: blob.to_vec(),
    };

    let expr = Expression::Literal(serde_to_jsonb(json!(10.5)));
    let val = evaluate_to_f64_lazy(&expr, &lazy);
    assert_eq!(val, Some(10.5));
}

#[test]
fn test_missing_math_functions() {
    let doc = serde_to_jsonb(json!({
        "one": 1.0,
        "zero": 0.0,
        "pi": std::f64::consts::PI,
        "e": std::f64::consts::E,
    }));

    let eval = |func: ScalarFunction, arg: f64| {
        let expr = Expression::Function {
            func,
            args: vec![Expression::Literal(serde_to_jsonb(json!(arg)))],
        };
        evaluate_expression(&expr, &doc)
    };

    // Acosh(1.0) -> 0.0
    let res = eval(ScalarFunction::Acosh, 1.0);
    assert!(res.as_f64().unwrap().abs() < 1e-10);

    // Cos(0) -> 1.0
    let res = eval(ScalarFunction::Cos, 0.0);
    assert!((res.as_f64().unwrap() - 1.0).abs() < 1e-10);

    // Cosh(0) -> 1.0
    let res = eval(ScalarFunction::Cosh, 0.0);
    assert!((res.as_f64().unwrap() - 1.0).abs() < 1e-10);

    // Sinh(0) -> 0.0
    let res = eval(ScalarFunction::Sinh, 0.0);
    assert!(res.as_f64().unwrap().abs() < 1e-10);

    // Tanh(0) -> 0.0
    let res = eval(ScalarFunction::Tanh, 0.0);
    assert!(res.as_f64().unwrap().abs() < 1e-10);

    // Log(100, 10) -> 2.0 (Log base 10 of 100)
    let expr = Expression::Function {
        func: ScalarFunction::Log,
        args: vec![
            Expression::Literal(serde_to_jsonb(json!(100.0))),
            Expression::Literal(serde_to_jsonb(json!(10.0))),
        ],
    };
    let res = evaluate_expression(&expr, &doc);
    assert!((res.as_f64().unwrap() - 2.0).abs() < 1e-10);

    // Log(e) -> Ln(e) -> 1.0 (Unary defaults to Ln)
    let expr = Expression::Function {
        func: ScalarFunction::Log,
        args: vec![Expression::Literal(serde_to_jsonb(json!(
            std::f64::consts::E
        )))],
    };
    let res = evaluate_expression(&expr, &doc);
    assert!((res.as_f64().unwrap() - 1.0).abs() < 1e-10);
}

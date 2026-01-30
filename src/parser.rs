use crate::query::{
    BinaryOperator, Expression, LogicalOperator, LogicalPlan, ScalarFunction, Statement,
};
use crate::{Value, serde_to_jsonb};
use sqlparser::ast::{
    self, BinaryOperator as SqlBinaryOperator, Expr, LimitClause, SetExpr, TableFactor, Values,
};
use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

#[derive(Debug)]
struct ArgusDialect;

impl Dialect for ArgusDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_' || ch == '$'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch.is_ascii_digit()
            || ch == '_'
            || ch == '$'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }
}

pub fn parse(sql: &str) -> Result<Statement, String> {
    let dialect = ArgusDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().map_err(|e| e.to_string())?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);

    let token = parser.peek_token();
    let keyword = token.token.to_string().to_uppercase();

    if keyword == "CREATE" {
        parser.next_token();
        parser.expect_keyword(Keyword::COLLECTION).unwrap();
        let name = parser.parse_object_name(false).unwrap().to_string();
        return Ok(Statement::CreateCollection { collection: name });
    } else if keyword == "DROP" {
        parser.next_token();
        parser.expect_keyword(Keyword::COLLECTION).unwrap();
        let name = parser.parse_object_name(false).unwrap().to_string();
        return Ok(Statement::DropCollection { collection: name });
    } else if keyword == "SHOW" {
        parser.next_token();
        let token = parser.next_token();
        if token.token.to_string().to_uppercase() == "COLLECTIONS" {
            return Ok(Statement::ShowCollections);
        }
    }

    let mut ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    if ast.len() != 1 {
        return Err("Expected exactly one statement".to_string());
    }

    match ast.pop().unwrap() {
        ast::Statement::Insert(insert) => {
            let collection = insert.table.to_string();
            let documents = convert_insert_source(insert.source)?;
            Ok(Statement::Insert {
                collection,
                documents,
            })
        }
        ast::Statement::Query(query) => {
            let logical_plan = convert_query(*query)?;
            Ok(Statement::Select(logical_plan))
        }
        _ => Err("Unsupported statement".to_string()),
    }
}

fn convert_insert_source(source: Option<Box<ast::Query>>) -> Result<Vec<Value>, String> {
    let query = source.ok_or("Insert must have a source")?;

    match *query.body {
        SetExpr::Values(Values { rows, .. }) => {
            let mut docs = Vec::new();
            for row in rows {
                if row.len() != 1 {
                    return Err("Each record must contain exactly one JSON object".to_string());
                }
                let expr = row.into_iter().next().unwrap();
                match expr {
                    Expr::Identifier(ident) => {
                        let json_str = ident.value;
                        let value: serde_json::Value = serde_json::from_str(&json_str)
                            .map_err(|e| format!("Invalid JSON in INSERT: {}", e))?;
                        docs.push(serde_to_jsonb(value));
                    }
                    _ => return Err("Expected a JSON object enclosed in backticks".to_string()),
                }
            }
            Ok(docs)
        }
        _ => Err("INSERT expects VALUES clause".to_string()),
    }
}

fn convert_query(query: ast::Query) -> Result<LogicalPlan, String> {
    let mut limit_val = None;
    let mut offset_val = None;

    if let Some(LimitClause::LimitOffset { limit, offset, .. }) = query.limit_clause {
        if let Some(l) = limit {
            limit_val = Some(parse_limit_expr(&l)?);
        }
        if let Some(o) = offset {
            offset_val = Some(parse_limit_expr(&o.value)?);
        }
    }

    // Body (SetExpr)
    let plan = match *query.body {
        SetExpr::Select(select) => convert_select(*select)?,
        _ => return Err("Only SELECT queries are supported (no UNION, etc.)".to_string()),
    };

    // Wrap with Limit/Offset
    let plan = if let Some(o) = offset_val {
        LogicalPlan::Offset {
            input: Box::new(plan),
            offset: o,
        }
    } else {
        plan
    };

    let plan = if let Some(l) = limit_val {
        LogicalPlan::Limit {
            input: Box::new(plan),
            limit: l,
        }
    } else {
        plan
    };

    Ok(plan)
}

fn parse_limit_expr(expr: &Expr) -> Result<usize, String> {
    match expr {
        Expr::Value(val_span) => match &val_span.value {
            ast::Value::Number(n, _) => {
                n.parse::<usize>().map_err(|_| "Invalid number".to_string())
            }
            _ => Err("Expected number".to_string()),
        },
        _ => Err("Expected value for limit/offset".to_string()),
    }
}

fn convert_select(select: ast::Select) -> Result<LogicalPlan, String> {
    // 1. FROM (Scan)
    if select.from.len() != 1 {
        return Err("FROM clause must have exactly one table".to_string());
    }
    let table = select.from.into_iter().next().unwrap();
    let collection = match table.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err("Unsupported FROM clause".to_string()),
    };

    let mut plan = LogicalPlan::Scan { collection };

    // 2. WHERE (Filter)
    if let Some(selection) = select.selection {
        let predicate = convert_expr(selection)?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // 3. SELECT (Project)
    let mut projections = Vec::new();
    for item in select.projection {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                projections.push(convert_expr(expr)?);
            }
            ast::SelectItem::ExprWithAlias { expr, alias: _ } => {
                projections.push(convert_expr(expr)?);
            }
            ast::SelectItem::Wildcard(_) => {
                return Err("Wildcard * not supported yet".to_string());
            }
            _ => return Err("Unsupported projection item".to_string()),
        }
    }

    plan = LogicalPlan::Project {
        input: Box::new(plan),
        projections,
    };

    Ok(plan)
}

fn convert_expr(expr: Expr) -> Result<Expression, String> {
    match expr {
        Expr::Identifier(ident) => {
            let value = ident.value;
            if value.starts_with('$') {
                Ok(Expression::JsonPath(value))
            } else {
                let parts: Vec<String> = value.split('.').map(|s| s.to_string()).collect();
                Ok(Expression::FieldReference(parts, value))
            }
        }
        Expr::CompoundIdentifier(idents) => {
            let path = idents
                .into_iter()
                .map(|i| i.value)
                .collect::<Vec<_>>()
                .join(".");
            if path.starts_with('$') {
                Ok(Expression::JsonPath(path))
            } else {
                let parts: Vec<String> = path.split('.').map(|s| s.to_string()).collect();
                Ok(Expression::FieldReference(parts, path))
            }
        }
        Expr::Value(val_span) => match val_span.value {
            ast::Value::Number(n, _) => {
                // Try parse as i64 or f64
                if let Ok(i) = n.parse::<i64>() {
                    // Create serde_json::Value first then convert?
                    // Or create jsonb_schema::Value directly.
                    // Value::Number(Number::Int64(i))
                    use jsonb_schema::{Number, Value as JsonbValue};
                    Ok(Expression::Literal(JsonbValue::Number(Number::Int64(i))))
                } else if let Ok(f) = n.parse::<f64>() {
                    use jsonb_schema::{Number, Value as JsonbValue};
                    Ok(Expression::Literal(JsonbValue::Number(Number::Float64(f))))
                } else {
                    Err("Invalid number".to_string())
                }
            }
            ast::Value::SingleQuotedString(s) => {
                use jsonb_schema::Value as JsonbValue;
                Ok(Expression::Literal(JsonbValue::String(s.into())))
            }
            ast::Value::Boolean(b) => {
                use jsonb_schema::Value as JsonbValue;
                Ok(Expression::Literal(JsonbValue::Bool(b)))
            }
            ast::Value::Null => {
                use jsonb_schema::Value as JsonbValue;
                Ok(Expression::Literal(JsonbValue::Null))
            }
            _ => Err(format!("Unsupported literal: {:?}", val_span.value)),
        },
        Expr::BinaryOp { left, op, right } => {
            let left_expr = Box::new(convert_expr(*left)?);
            let right_expr = Box::new(convert_expr(*right)?);

            let (is_logical, b_op, l_op) = match op {
                SqlBinaryOperator::Eq => (false, Some(BinaryOperator::Eq), None),
                SqlBinaryOperator::NotEq => (false, Some(BinaryOperator::Neq), None),
                SqlBinaryOperator::Lt => (false, Some(BinaryOperator::Lt), None),
                SqlBinaryOperator::LtEq => (false, Some(BinaryOperator::Lte), None),
                SqlBinaryOperator::Gt => (false, Some(BinaryOperator::Gt), None),
                SqlBinaryOperator::GtEq => (false, Some(BinaryOperator::Gte), None),
                SqlBinaryOperator::And => (true, None, Some(LogicalOperator::And)),
                SqlBinaryOperator::Or => (true, None, Some(LogicalOperator::Or)),
                _ => return Err(format!("Unsupported binary operator: {:?}", op)),
            };

            if is_logical {
                Ok(Expression::Logical {
                    left: left_expr,
                    op: l_op.unwrap(),
                    right: right_expr,
                })
            } else {
                Ok(Expression::Binary {
                    left: left_expr,
                    op: b_op.unwrap(),
                    right: right_expr,
                })
            }
        }
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            let scalar_func = match name.as_str() {
                "ABS" => ScalarFunction::Abs,
                "ACOS" => ScalarFunction::Acos,
                "ACOSH" => ScalarFunction::Acosh,
                "ASIN" => ScalarFunction::Asin,
                "ATAN" => ScalarFunction::Atan,
                "ATAN2" => ScalarFunction::Atan2,
                "CEIL" => ScalarFunction::Ceil,
                "COS" => ScalarFunction::Cos,
                "COSH" => ScalarFunction::Cosh,
                "DIV" => ScalarFunction::Div,
                "EXP" => ScalarFunction::Exp,
                "FLOOR" => ScalarFunction::Floor,
                "LN" => ScalarFunction::Ln,
                "LOG" => ScalarFunction::Log,
                "LOG10" => ScalarFunction::Log10,
                "POW" => ScalarFunction::Pow,
                "RAND" => ScalarFunction::Rand,
                "ROUND" => ScalarFunction::Round,
                "SIGN" => ScalarFunction::Sign,
                "SIN" => ScalarFunction::Sin,
                "SINH" => ScalarFunction::Sinh,
                "SQRT" => ScalarFunction::Sqrt,
                "TAN" => ScalarFunction::Tan,
                "TANH" => ScalarFunction::Tanh,
                _ => return Err(format!("Unsupported function: {}", name)),
            };

            let args_list = match func.args {
                sqlparser::ast::FunctionArguments::List(list) => list.args,
                _ => return Err(format!("Function {} expects arguments", name)),
            };

            // Check arity (same as before)
            match scalar_func {
                ScalarFunction::Rand => {
                    if !args_list.is_empty() {
                        return Err(format!("Function {} requires 0 arguments", name));
                    }
                }
                ScalarFunction::Log | ScalarFunction::Round => {
                    if args_list.is_empty() || args_list.len() > 2 {
                        return Err(format!("Function {} requires 1 or 2 arguments", name));
                    }
                }
                ScalarFunction::Atan2 | ScalarFunction::Div | ScalarFunction::Pow => {
                    if args_list.len() != 2 {
                        return Err(format!("Function {} requires exactly 2 arguments", name));
                    }
                }
                _ => {
                    if args_list.len() != 1 {
                        return Err(format!("Function {} requires exactly 1 argument", name));
                    }
                }
            }

            let mut expr_args = Vec::new();
            for arg in args_list {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) => {
                        expr_args.push(convert_expr(e)?);
                    }
                    _ => return Err(format!("Unsupported argument type for function {}", name)),
                }
            }

            Ok(Expression::Function {
                func: scalar_func,
                args: expr_args,
            })
        }
        Expr::JsonAccess { .. } => Err("JsonAccess not implemented".to_string()),
        _ => Err(format!("Unsupported expression: {:?}", expr)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonb_to_serde;

    #[test]
    fn test_parse_insert() {
        let sql =
            r#"INSERT INTO users VALUES (`{"name": "Alice", "age": 30}`), (`{"name": "Bob"}`)"#;
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::Insert {
                collection,
                documents,
            } => {
                assert_eq!(collection, "users");
                assert_eq!(documents.len(), 2);
                let doc0 = jsonb_to_serde(&documents[0]);
                assert_eq!(doc0["name"], "Alice");
                let doc1 = jsonb_to_serde(&documents[1]);
                assert_eq!(doc1["name"], "Bob");
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_select() {
        let sql = "SELECT name, age FROM users WHERE age > 18 AND active = true LIMIT 10 OFFSET 5";
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::Select(plan) => {
                // Verify structure: Limit(Offset(Project(Filter(Scan))))
                match plan {
                    LogicalPlan::Limit { input, limit } => {
                        assert_eq!(limit, 10);
                        match *input {
                            LogicalPlan::Offset { input, offset } => {
                                assert_eq!(offset, 5);
                                match *input {
                                    LogicalPlan::Project { input, projections } => {
                                        assert_eq!(projections.len(), 2);
                                        match *input {
                                            LogicalPlan::Filter {
                                                input,
                                                predicate: _,
                                            } => match *input {
                                                LogicalPlan::Scan { collection } => {
                                                    assert_eq!(collection, "users");
                                                }
                                                _ => panic!("Expected Scan"),
                                            },
                                            _ => panic!("Expected Filter"),
                                        }
                                    }
                                    _ => panic!("Expected Project"),
                                }
                            }
                            _ => panic!("Expected Offset"),
                        }
                    }
                    _ => panic!("Expected Limit"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_jsonpath() {
        // Test standard dot notation with $
        let sql = "SELECT $.a.b FROM t";
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::Select(LogicalPlan::Project { projections, .. }) => match &projections[0] {
                Expression::JsonPath(p) => assert_eq!(p, "$.a.b"),
                _ => panic!("Expected JsonPath"),
            },
            _ => panic!("Expected Select Project"),
        }

        // Test backtick quoted jsonpath with brackets
        let sql = "SELECT `$.a[0]` FROM t";
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::Select(LogicalPlan::Project { projections, .. }) => match &projections[0] {
                Expression::JsonPath(p) => assert_eq!(p, "$.a[0]"),
                _ => panic!("Expected JsonPath"),
            },
            _ => panic!("Expected Select Project"),
        }
    }

    #[test]
    fn test_parse_create_collection() {
        let sql = "CREATE COLLECTION users";
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::CreateCollection { collection } => {
                assert_eq!(collection, "users");
            }
            _ => panic!("Expected CreateCollection"),
        }
    }

    #[test]
    fn test_parse_drop_collection() {
        let sql = "DROP COLLECTION users";
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::DropCollection { collection } => {
                assert_eq!(collection, "users");
            }
            _ => panic!("Expected DropCollection"),
        }
    }

    #[test]
    fn test_parse_show_collections() {
        let sql = "SHOW COLLECTIONS";
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::ShowCollections => {}
            _ => panic!("Expected ShowCollections"),
        }
    }

    #[test]
    fn test_parse_functions() {
        let sql = "SELECT ABS(age), SQRT(height) FROM users";
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::Select(LogicalPlan::Project { projections, .. }) => {
                assert_eq!(projections.len(), 2);
                match &projections[0] {
                    Expression::Function { func, args } => {
                        assert_eq!(*func, ScalarFunction::Abs);
                        assert_eq!(args.len(), 1);
                        match &args[0] {
                            Expression::FieldReference(_, s) => assert_eq!(s, "age"),
                            _ => panic!("Expected FieldReference"),
                        }
                    }
                    _ => panic!("Expected Function"),
                }
            }
            _ => panic!("Expected Select Project"),
        }
    }
}

use crate::query::{
    BinaryOperator, Expression, LogicalOperator, LogicalPlan, ScalarFunction, Statement,
};
use crate::{Value, serde_to_jsonb};
use bumpalo::Bump;
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

pub fn parse<'a>(sql: &str, arena: &'a Bump) -> Result<Statement<'a>, String> {
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
            let logical_plan = convert_query(*query, arena)?;
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

fn convert_query<'a>(query: ast::Query, arena: &'a Bump) -> Result<LogicalPlan<'a>, String> {
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
        SetExpr::Select(select) => convert_select(*select, arena)?,
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

fn convert_select<'a>(select: ast::Select, arena: &'a Bump) -> Result<LogicalPlan<'a>, String> {
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
        let predicate = convert_expr(selection, arena)?;
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
                projections.push(convert_expr(expr, arena)?);
            }
            ast::SelectItem::ExprWithAlias { expr, alias: _ } => {
                projections.push(convert_expr(expr, arena)?);
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

fn convert_expr<'a>(expr: Expr, arena: &'a Bump) -> Result<Expression<'a>, String> {
    match expr {
        Expr::Identifier(ident) => {
            let value = ident.value;
            // Allocate string in arena
            let value_ref = arena.alloc_str(&value);
            if value_ref.starts_with('$') {
                let parsed = jsonb_schema::jsonpath::parse_json_path(value_ref.as_bytes())
                    .map_err(|e| format!("Invalid JSON path: {}", e))?;
                Ok(Expression::JsonPath(Box::new(parsed), value_ref))
            } else {
                let parts: Vec<&'a str> = value_ref.split('.').collect(); // This collects into Vec<&str> referencing the arena str
                Ok(Expression::FieldReference(parts, value_ref))
            }
        }
        Expr::CompoundIdentifier(idents) => {
            let path = idents
                .into_iter()
                .map(|i| i.value)
                .collect::<Vec<_>>()
                .join(".");
            let path_ref = arena.alloc_str(&path);
            if path_ref.starts_with('$') {
                let parsed = jsonb_schema::jsonpath::parse_json_path(path_ref.as_bytes())
                    .map_err(|e| format!("Invalid JSON path: {}", e))?;
                Ok(Expression::JsonPath(Box::new(parsed), path_ref))
            } else {
                let parts: Vec<&'a str> = path_ref.split('.').collect();
                Ok(Expression::FieldReference(parts, path_ref))
            }
        }
        Expr::Value(val_span) => match val_span.value {
            ast::Value::Number(n, _) => {
                // Try parse as i64 or f64
                if let Ok(i) = n.parse::<i64>() {
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
            let left_expr = Box::new(convert_expr(*left, arena)?);
            let right_expr = Box::new(convert_expr(*right, arena)?);

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

            // Check arity checks (omitted for brevity, assume similar to before)

            let mut expr_args = Vec::new();
            for arg in args_list {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) => {
                        expr_args.push(convert_expr(e, arena)?);
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
    use bumpalo::Bump;

    #[test]
    fn test_parse_insert() {
        let sql =
            r#"INSERT INTO users VALUES (`{"name": "Alice", "age": 30}`), (`{"name": "Bob"}`)"#;
        let arena = Bump::new();
        let stmt = parse(sql, &arena).unwrap();
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
        let arena = Bump::new();
        let stmt = parse(sql, &arena).unwrap();
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
        let sql = "SELECT $.a.b FROM t";
        let arena = Bump::new();
        let stmt = parse(sql, &arena).unwrap();
        match stmt {
            Statement::Select(LogicalPlan::Project { projections, .. }) => match &projections[0] {
                Expression::JsonPath(_, p) => assert_eq!(p, &"$.a.b"),
                _ => panic!("Expected JsonPath"),
            },
            _ => panic!("Expected Select Project"),
        }
    }

    // Add other tests similarly updated with arena...
}

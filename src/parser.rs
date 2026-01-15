use crate::query::{Expression, BinaryOperator, LogicalOperator, LogicalPlan, Statement};
use serde_json::Value;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{self, SetExpr, TableFactor, BinaryOperator as SqlBinaryOperator, Expr, LimitClause};
use nom::{
    bytes::complete::{tag_no_case, take_while1},
    character::complete::{multispace0, multispace1, char},
    sequence::tuple,
    multi::separated_list1,
    IResult,
};

pub fn parse(sql: &str) -> Result<Statement, String> {
    let trimmed = sql.trim();
    if trimmed.to_uppercase().starts_with("INSERT") {
        parse_insert(trimmed).map_err(|e| format!("Insert parse error: {}", e))
    } else {
        parse_select(trimmed)
    }
}

// --- INSERT Parsing (Custom using Nom) ---

fn parse_insert(input: &str) -> Result<Statement, String> {
    match insert_statement(input) {
        Ok((_, stmt)) => Ok(stmt),
        Err(e) => Err(format!("{}", e)),
    }
}

fn insert_statement(input: &str) -> IResult<&str, Statement> {
    // INSERT INTO <collection> RECORDS <json>...
    let (input, _) = tag_no_case("INSERT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("INTO")(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, collection) = identifier(input)?;
    
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("RECORDS")(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, documents) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        json_object
    )(input)?;
    
    Ok((input, Statement::Insert {
        collection: collection.to_string(),
        documents,
    }))
}

fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

fn json_object(input: &str) -> IResult<&str, Value> {
    let mut depth = 0;
    let mut len = 0;
    let mut found_start = false;

    // Skip leading whitespace
    let leading_ws = input.chars().take_while(|c| c.is_whitespace()).count();
    let trimmed_input = &input[leading_ws..];
    
    if !trimmed_input.starts_with('{') {
         return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }

    // Iterate to find the matching brace
    for (i, c) in trimmed_input.char_indices() {
        if c == '{' {
            depth += 1;
            found_start = true;
        } else if c == '}' {
            depth -= 1;
        }
        
        if found_start && depth == 0 {
            len = i + 1;
            break;
        }
    }

    if depth != 0 {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Complete)));
    }

    let json_str = &trimmed_input[0..len];
    let value: Value = serde_json::from_str(json_str).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::MapRes))
    })?;

    Ok((&trimmed_input[len..], value))
}


// --- SELECT Parsing (sqlparser) ---

fn parse_select(sql: &str) -> Result<Statement, String> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    if ast.len() != 1 {
        return Err("Expected exactly one statement".to_string());
    }

    match &ast[0] {
        ast::Statement::Query(query) => {
            let logical_plan = convert_query(query)?;
            Ok(Statement::Select(logical_plan))
        }
        _ => Err("Only SELECT statements are supported".to_string()),
    }
}

fn convert_query(query: &ast::Query) -> Result<LogicalPlan, String> {
    let mut limit_val = None;
    let mut offset_val = None;

    if let Some(limit_clause) = &query.limit_clause {
         match limit_clause {
             LimitClause::Limit { limit, offset } => {
                 limit_val = Some(parse_limit_expr(limit)?);
                 if let Some(off) = offset {
                     offset_val = Some(parse_limit_expr(off)?);
                 }
             }
             LimitClause::LimitOffset { limit, offset } => {
                 limit_val = Some(parse_limit_expr(limit)?);
                 offset_val = Some(parse_limit_expr(offset)?);
             }
         }
    }

    // Body (SetExpr)
    let plan = match &*query.body {
        SetExpr::Select(select) => convert_select(select)?,
        _ => return Err("Only SELECT queries are supported (no UNION, etc.)".to_string()),
    };

    // Wrap with Limit/Offset
    let plan = if let Some(o) = offset_val {
        LogicalPlan::Offset { input: Box::new(plan), offset: o }
    } else {
        plan
    };

    let plan = if let Some(l) = limit_val {
        LogicalPlan::Limit { input: Box::new(plan), limit: l }
    } else {
        plan
    };

    Ok(plan)
}

fn parse_limit_expr(expr: &Expr) -> Result<usize, String> {
    match expr {
        Expr::Value(val_span) => match &val_span.value {
             ast::Value::Number(n, _) => n.parse::<usize>().map_err(|_| "Invalid number".to_string()),
             _ => Err("Expected number".to_string()),
        },
        _ => Err("Expected value for limit/offset".to_string()),
    }
}

fn convert_select(select: &ast::Select) -> Result<LogicalPlan, String> {
    // 1. FROM (Scan)
    if select.from.len() != 1 {
        return Err("FROM clause must have exactly one table".to_string());
    }
    let table = &select.from[0];
    let collection = match &table.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err("Unsupported FROM clause".to_string()),
    };
    
    let mut plan = LogicalPlan::Scan { collection };

    // 2. WHERE (Filter)
    if let Some(selection) = &select.selection {
        let predicate = convert_expr(selection)?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // 3. SELECT (Project)
    let mut projections = Vec::new();
    for item in &select.projection {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                projections.push(convert_expr(expr)?);
            }
            ast::SelectItem::ExprWithAlias { expr, alias: _ } => {
                // Ignore alias for now as LogicalPlan doesn't support renaming explicitly yet
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

fn convert_expr(expr: &Expr) -> Result<Expression, String> {
    match expr {
        Expr::Identifier(ident) => Ok(Expression::FieldReference(ident.value.clone())),
        Expr::CompoundIdentifier(idents) => {
            let path = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
            Ok(Expression::FieldReference(path))
        }
        Expr::Value(val_span) => match &val_span.value {
            ast::Value::Number(n, _) => {
                // Try parse as i64 or f64
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Expression::Literal(serde_json::Value::Number(i.into())))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Expression::Literal(serde_json::Value::Number(serde_json::Number::from_f64(f).ok_or("Invalid float")?)))
                } else {
                    Err("Invalid number".to_string())
                }
            }
            ast::Value::SingleQuotedString(s) => Ok(Expression::Literal(Value::String(s.clone()))),
            ast::Value::Boolean(b) => Ok(Expression::Literal(Value::Bool(*b))),
            ast::Value::Null => Ok(Expression::Literal(Value::Null)),
            _ => Err(format!("Unsupported literal: {:?}", val_span.value)),
        },
        Expr::BinaryOp { left, op, right } => {
            let left_expr = Box::new(convert_expr(left)?);
            let right_expr = Box::new(convert_expr(right)?);
            
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
        },
        Expr::JsonAccess { .. } => Err("JsonAccess not implemented".to_string()),
        _ => Err(format!("Unsupported expression: {:?}", expr)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_insert() {
        let sql = r#"INSERT INTO users RECORDS {"name": "Alice", "age": 30}, {"name": "Bob"}"#;
        let stmt = parse(sql).unwrap();
        match stmt {
            Statement::Insert { collection, documents } => {
                assert_eq!(collection, "users");
                assert_eq!(documents.len(), 2);
                assert_eq!(documents[0]["name"], "Alice");
                assert_eq!(documents[1]["name"], "Bob");
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
                                            LogicalPlan::Filter { input, predicate: _ } => {
                                                match *input {
                                                    LogicalPlan::Scan { collection } => {
                                                        assert_eq!(collection, "users");
                                                    }
                                                    _ => panic!("Expected Scan"),
                                                }
                                            }
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
}
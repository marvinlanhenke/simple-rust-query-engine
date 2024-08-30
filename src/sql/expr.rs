use std::sync::Arc;

use snafu::location;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Value,
};

use crate::{
    error::{Error, Result},
    expression::{
        logical::{
            aggregate::{Aggregate, AggregateFunction},
            binary::Binary,
            column::Column,
            expr::Expression,
            expr_fn::lit,
        },
        operator::Operator,
        values::ScalarValue,
    },
};

pub fn sql_expr_to_logical_expr(expr: &Expr) -> Result<Expression> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let lhs = sql_expr_to_logical_expr(left)?;
            let rhs = sql_expr_to_logical_expr(right)?;
            let op = parse_sql_binary_operator(op)?;

            Ok(Expression::Binary(Binary::new(
                Arc::new(lhs),
                op,
                Arc::new(rhs),
            )))
        }
        Expr::Identifier(ident) => Ok(Expression::Column(Column::new(ident.value.clone()))),
        Expr::Value(value) => match value {
            Value::Null => Ok(Expression::Literal(ScalarValue::Null)),
            Value::Boolean(b) => Ok(Expression::Literal(ScalarValue::Boolean(Some(*b)))),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(lit(s)),
            Value::Number(n, _) => {
                if let Ok(n) = n.parse::<i64>() {
                    return Ok(lit(n));
                }

                if let Ok(n) = n.parse::<u64>() {
                    return Ok(lit(n));
                }

                if let Ok(n) = n.parse::<f64>() {
                    Ok(Expression::Literal(ScalarValue::Float64(Some(n))))
                } else {
                    Err(Error::InvalidData {
                        message: format!("Cannot parse {n} as number"),
                        location: location!(),
                    })
                }
            }
            _ => Err(Error::InvalidOperation {
                message: format!("SQL value expression {} is not supported yet", value),
                location: location!(),
            }),
        },
        Expr::Function(sql_func) => {
            let ObjectName(ident) = sql_func.name.clone();
            let func = match ident.len() {
                1 => AggregateFunction::try_from(ident[0].value.as_ref())?,
                _ => {
                    return Err(Error::InvalidOperation {
                        message: "SQL function expression expects 1 ident".to_string(),
                        location: location!(),
                    })
                }
            };

            let expression = match &sql_func.args {
                FunctionArguments::List(list) => {
                    let func_arg = list.args[0].clone();
                    match func_arg {
                        FunctionArg::Unnamed(fe) => match fe {
                            FunctionArgExpr::Expr(e) => sql_expr_to_logical_expr(&e)?,
                            _ => {
                                return Err(Error::InvalidOperation {
                                    message: format!(
                                        "SQL function argument {} is not supported yet",
                                        fe
                                    ),
                                    location: location!(),
                                })
                            }
                        },
                        _ => {
                            return Err(Error::InvalidOperation {
                                message: format!(
                                    "SQL function argument {} is not supported yet",
                                    func_arg
                                ),
                                location: location!(),
                            })
                        }
                    }
                }
                _ => {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "SQL function argument {} is not supported yet",
                            sql_func.args
                        ),
                        location: location!(),
                    })
                }
            };

            Ok(Expression::Aggregate(Aggregate::new(
                func,
                Arc::new(expression),
            )))
        }
        _ => Err(Error::InvalidOperation {
            message: format!("SQL expression {} is not supported yet", expr),
            location: location!(),
        }),
    }
}

pub fn parse_sql_binary_operator(op: &BinaryOperator) -> Result<Operator> {
    match op {
        BinaryOperator::Gt => Ok(Operator::Gt),
        BinaryOperator::GtEq => Ok(Operator::GtEq),
        BinaryOperator::Lt => Ok(Operator::Lt),
        BinaryOperator::LtEq => Ok(Operator::LtEq),
        BinaryOperator::Eq => Ok(Operator::Eq),
        BinaryOperator::NotEq => Ok(Operator::NotEq),
        BinaryOperator::Plus => Ok(Operator::Plus),
        BinaryOperator::Minus => Ok(Operator::Minus),
        BinaryOperator::Multiply => Ok(Operator::Multiply),
        BinaryOperator::Divide => Ok(Operator::Divide),
        BinaryOperator::And => Ok(Operator::And),
        BinaryOperator::Or => Ok(Operator::Or),
        _ => Err(Error::InvalidOperation {
            message: format!("SQL operator {} is not supported yet", op),
            location: location!(),
        }),
    }
}

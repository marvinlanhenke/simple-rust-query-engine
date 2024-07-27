use std::fmt::Display;

use crate::{
    error::{Error, Result},
    expression::{coercion::Signature, operator::Operator, values::ScalarValue},
    plan::logical::plan::LogicalPlan,
};
use arrow::datatypes::{DataType, Field, Schema};
use snafu::location;

use super::{binary::Binary, column::Column, expr_fn::binary_expr};

/// Represents a logical [`Expression`] in an AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expression {
    /// A [`Column`] expression.
    Column(Column),
    /// A [`ScalarValue`] expression.
    Literal(ScalarValue),
    /// A [`Binary`] expression.
    Binary(Binary),
}

impl Expression {
    /// Resolves this column to its [`Field`] definition from a logical plan.
    pub fn to_field(&self, plan: &LogicalPlan) -> Result<Field> {
        use Expression::*;

        match self {
            Column(e) => e.to_field_from_plan(plan),
            other => Err(Error::InvalidOperation {
                message: format!(
                    "The conversion of expression '{}' to field is not supported",
                    other
                ),
                location: location!(),
            }),
        }
    }

    /// Returns the [`DataType`] of the expression.
    pub fn data_type(&self, schema: &Schema) -> Result<DataType> {
        use Expression::*;

        match self {
            Column(e) => Ok(e.to_field(schema)?.data_type().clone()),
            Literal(e) => Ok(e.data_type()),
            Binary(e) => {
                let lhs = e.lhs().data_type(schema)?;
                let rhs = e.rhs().data_type(schema)?;
                Signature::get_result_type(&lhs, e.op(), &rhs)
            }
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expression::*;

        match self {
            Column(e) => write!(f, "{}", e),
            Literal(e) => write!(f, "{}", e),
            Binary(e) => write!(f, "{}", e),
        }
    }
}

macro_rules! make_expr_fn {
    ($fn:ident, $op:ident) => {
        impl Expression {
            pub fn $fn(self, other: Expression) -> Expression {
                binary_expr(self, Operator::$op, other)
            }
        }
    };
}

make_expr_fn!(eq, Eq);
make_expr_fn!(neq, NotEq);
make_expr_fn!(lt, Lt);
make_expr_fn!(lt_eq, LtEq);
make_expr_fn!(gt, Gt);
make_expr_fn!(gt_eq, GtEq);
make_expr_fn!(and, And);
make_expr_fn!(or, Or);

#[cfg(test)]
mod tests {}

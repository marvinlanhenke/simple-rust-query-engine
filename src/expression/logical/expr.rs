use std::fmt::Display;

use crate::{
    error::{Error, Result},
    expression::values::ScalarValue,
    plan::logical::plan::LogicalPlan,
};
use arrow::datatypes::{DataType, Field, Schema};
use snafu::location;

use super::column::Column;

/// Represents a logical [`Expression`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expression {
    /// A [`Column`] expression in an AST.
    Column(Column),
    /// A [`ScalarValue`] expression in an AST.
    Literal(ScalarValue),
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
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expression::*;

        match self {
            Column(e) => write!(f, "{}", e),
            Literal(e) => write!(f, "{}", e),
        }
    }
}

#[cfg(test)]
mod tests {}

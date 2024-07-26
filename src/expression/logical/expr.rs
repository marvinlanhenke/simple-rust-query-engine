use std::fmt::Display;

use crate::{error::Result, expression::scalar::ScalarValue, plan::logical::plan::LogicalPlan};
use arrow::datatypes::Field;

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
            _ => todo!(),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expression::*;

        match self {
            Column(e) => write!(f, "{}", e),
            Literal(_) => write!(f, "TODO!"),
        }
    }
}

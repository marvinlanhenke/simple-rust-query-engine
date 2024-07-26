use std::fmt::Display;

use crate::{error::Result, expression::scalar::ScalarValue, plan::logical::plan::LogicalPlan};
use arrow::datatypes::Field;

use super::column::Column;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expression {
    Column(Column),
    Literal(ScalarValue),
}

impl Expression {
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

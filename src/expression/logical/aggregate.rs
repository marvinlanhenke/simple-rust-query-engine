use std::{fmt::Display, sync::Arc};

use crate::error::Result;
use arrow::datatypes::DataType;

use super::expr::Expression;

/// Represents aggregate functions
/// that can be applied with [`Aggregate`] expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    /// The count function (e.g. `SELECT COUNT(*) FROM a;`)
    Count,
}

impl AggregateFunction {
    /// Returns the name of the aggregate function as a string.
    pub fn name(&self) -> &str {
        match self {
            AggregateFunction::Count => "COUNT",
        }
    }

    /// Returns the result data type of the aggregate function.
    pub fn result_type(&self) -> Result<DataType> {
        match self {
            AggregateFunction::Count => Ok(DataType::Int64),
        }
    }
}

/// Represents an [`Aggregate`] expression
/// combining an [`AggregateFunction`] and an [`Expression`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Aggregate {
    /// The aggregate function to be applied.
    func: AggregateFunction,
    /// The expression to aggregate.
    expression: Arc<Expression>,
}

impl Aggregate {
    /// Creates a new [`Aggregate`] instance.
    pub fn new(func: AggregateFunction, expression: Arc<Expression>) -> Self {
        Self { func, expression }
    }

    /// Returns the name of the aggregate function.
    pub fn name(&self) -> &str {
        self.func.name()
    }

    /// Returns the result data type of the aggregate function.
    pub fn result_type(&self) -> Result<DataType> {
        self.func.result_type()
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name(), self.expression)
    }
}

use std::{fmt::Display, sync::Arc};

use crate::error::Result;
use arrow::datatypes::DataType;

use super::expr::Expression;

/// Represents aggregate functions
/// that can be applied with [`Aggregate`] expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    /// A count function (e.g. `SELECT COUNT(*) FROM a;`)
    Count,
    /// A sum function (e.g. `SELECT SUM(c1) from t;`)
    Sum,
    /// An avg function (e.g. `SELECT AVG(c1) from t;`)
    Avg,
}

impl AggregateFunction {
    /// Returns the name of the aggregate function as a string.
    pub fn name(&self) -> &str {
        match self {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
        }
    }

    /// Returns the result data type of the aggregate function.
    pub fn result_type(&self) -> Result<DataType> {
        match self {
            AggregateFunction::Count => Ok(DataType::Int64),
            AggregateFunction::Sum => Ok(DataType::Int64),
            AggregateFunction::Avg => Ok(DataType::Float64),
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

    /// The aggregate function to be applied.
    pub fn func(&self) -> &AggregateFunction {
        &self.func
    }

    /// The expression to aggregate.
    pub fn expression(&self) -> &Expression {
        self.expression.as_ref()
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

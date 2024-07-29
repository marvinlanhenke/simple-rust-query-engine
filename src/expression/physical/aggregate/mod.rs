use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{array::ArrayRef, datatypes::Field};

use crate::{error::Result, expression::values::ScalarValue};

use super::expr::PhysicalExpression;

pub mod count;

/// Tracks an aggregate function's state.
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the intermediate state of the accumulator,
    /// consuming the intermediate state.
    fn state(&mut self) -> Result<Vec<ScalarValue>>;

    /// Returns the final aggregate value.
    fn eval(&mut self) -> Result<ScalarValue>;

    /// Updates the accumulator's state from its input.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()>;

    /// Updates the accumulator's state from an `Array`.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;
}

/// A trait that represents an [`AggregateExpr`].
pub trait AggregateExpr: Send + Sync + Debug {
    /// Returns the aggregate expression as [`Any`].
    fn as_any(&self) -> &dyn Any;

    /// The field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// The accumulator used to accumulate values from the expression.
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// The accumulator's state fields.
    fn state_fields(&self) -> Result<Vec<Field>>;

    /// The expressions that are used by the accumulator.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpression>>;
}

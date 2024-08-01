use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{array::ArrayRef, datatypes::Field};

use crate::{error::Result, expression::values::ScalarValue};

use super::expr::PhysicalExpression;

pub mod average;
pub mod count;
pub mod min_max;
pub mod sum;

/// Tracks an aggregate function's state.
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the intermediate state of the accumulator,
    /// consuming the intermediate state.
    fn state(&mut self) -> Result<Vec<ScalarValue>>;

    /// Returns the final aggregate value.
    fn eval(&mut self) -> Result<ScalarValue>;

    /// Updates the accumulator's state from its input.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()>;
}

/// Tracks an aggregate function's state with groupings.
pub trait GroupAccumulator: Send + Sync + Debug {
    /// Returns the intermediate state of the accumulator,
    /// consuming the intermediate state.
    fn state(&mut self) -> Result<Vec<ArrayRef>>;

    /// Returns the final aggregate value.
    fn eval(&mut self) -> Result<ArrayRef>;

    /// Updates the accumulator's state from its input, which belongs to a list of group indices.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()>;
}

/// A trait that represents an [`AggregateExpr`].
pub trait AggregateExpr: Send + Sync + Debug {
    /// Returns the aggregate expression as [`Any`].
    fn as_any(&self) -> &dyn Any;

    /// The field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// The accumulator used to accumulate values from the expression.
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// Indicates if an accumulator with groupings is supported.
    fn group_accumulator_supported(&self) -> bool {
        false
    }

    /// The accumulator used to accumulate values from the expression with groupings.
    fn create_group_accumulator(&self) -> Result<Box<dyn GroupAccumulator>>;

    /// The accumulator's state fields.
    fn state_fields(&self) -> Result<Vec<Field>>;

    /// The expressions that are used by the accumulator.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpression>>;
}

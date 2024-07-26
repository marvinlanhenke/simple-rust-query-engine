use std::{
    any::Any,
    fmt::{Debug, Display},
};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Schema},
};

use crate::{error::Result, expression::scalar::ColumnarValue};

/// A trait representing a [`PhysicalExpression`] in an [`ExecutionPlan`].
pub trait PhysicalExpression: Display + Debug + Send + Sync {
    /// Returns a reference to self as `dyn Any`.
    fn as_any(&self) -> &dyn Any;

    /// Returns the [`DataType`] of the expression evaluated agains the schema.
    fn data_type(&self, schema: &Schema) -> Result<DataType>;

    /// Evaluates the expression against the input [`RecordBatch`].
    fn eval(&self, input: &RecordBatch) -> Result<ColumnarValue>;
}

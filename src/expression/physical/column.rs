use std::{any::Any, fmt::Display};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Schema},
};
use snafu::location;

use crate::error::{Error, Result};

use super::expr::PhysicalExpression;

/// Represents a physical [`Column`]
/// expression in an [`ExecutionPlan`]
#[derive(Debug)]
pub struct ColumnExpr {
    /// The column index.
    index: usize,
}

impl ColumnExpr {
    /// Creates a new [`ColumnExpr`] instance.
    pub fn new(index: usize) -> Self {
        Self { index }
    }

    /// The column index.
    pub fn index(&self) -> usize {
        self.index
    }
}

impl PhysicalExpression for ColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        if self.index >= schema.fields().len() {
            return Err(Error::InvalidData {
                message: "Referenced column cannot be found in schema. Index out of bound."
                    .to_string(),
                location: location!(),
            });
        };

        Ok(schema.field(self.index).data_type().clone())
    }

    fn eval(&self, _input: &RecordBatch) -> Result<()> {
        Ok(())
    }
}

impl Display for ColumnExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.index)
    }
}

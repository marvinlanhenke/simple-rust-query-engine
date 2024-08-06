use std::{any::Any, fmt::Display, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};

use super::expr::PhysicalExpression;

use crate::{error::Result, expression::values::ColumnarValue};

/// Represents a physical [`SortExpr`] in a query.
#[derive(Debug)]
pub struct SortExpr {
    /// The sort expression to be evaluated.
    expression: Arc<dyn PhysicalExpression>,
    /// Indicates whether the sort order is ascending or not.
    ascending: bool,
}

impl SortExpr {
    /// Creates a new [`SortExpr`] instance.
    pub fn new(expression: Arc<dyn PhysicalExpression>, ascending: bool) -> Self {
        Self {
            expression,
            ascending,
        }
    }

    /// Retrieves the sort expression to be evaluated.
    pub fn expression(&self) -> &dyn PhysicalExpression {
        self.expression.as_ref()
    }

    /// Retrieves the sort order.
    pub fn ascending(&self) -> bool {
        self.ascending
    }
}

impl PhysicalExpression for SortExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        self.expression.data_type(schema)
    }

    fn nullable(&self, schema: &Schema) -> Result<bool> {
        self.expression.nullable(schema)
    }

    fn eval(&self, input: &RecordBatch) -> Result<ColumnarValue> {
        let sort_value = self.expression.eval(input)?;
        let sort_array = match sort_value {
            ColumnarValue::Array(v) => v,
            ColumnarValue::Scalar(v) => v.to_array(input.num_rows()),
        };

        Ok(ColumnarValue::Array(sort_array))
    }
}

impl Display for SortExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let order_str = if self.ascending { "ASC" } else { "DESC" };
        write!(f, "{} {}", self.expression, order_str)
    }
}

#[cfg(test)]
mod tests {}

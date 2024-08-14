use std::{any::Any, fmt::Display, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Schema},
};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::values::ColumnarValue,
};

use super::expr::PhysicalExpression;

/// Represents a physical [`Column`]
/// at a given index in a [`RecordBatch`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnExpr {
    /// The column name.
    name: String,
    /// The column index.
    index: usize,
}

impl ColumnExpr {
    /// Creates a new [`ColumnExpr`] instance.
    pub fn new(name: impl Into<String>, index: usize) -> Self {
        Self {
            name: name.into(),
            index,
        }
    }

    /// The column name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
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

    fn nullable(&self, schema: &Schema) -> Result<bool> {
        Ok(schema.field(self.index).is_nullable())
    }

    /// Evaluates the column index against the input `RecordBatch`.
    /// Return a [`ColumnarValue`] wrapping the underlying arrow array,
    /// representing the projected column with its data.
    fn eval(&self, input: &RecordBatch) -> Result<ColumnarValue> {
        let array = Arc::new(input.column(self.index).clone());

        Ok(ColumnarValue::Array(array))
    }
}

impl Display for ColumnExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::DataType;

    use crate::{
        expression::{physical::expr::PhysicalExpression, values::ColumnarValue},
        tests::{create_record_batch, create_schema},
    };

    use super::ColumnExpr;

    #[test]
    fn test_column_expr_eval() {
        let batch = create_record_batch();
        let expr = ColumnExpr::new("a", 0);

        let result = expr.eval(&batch).unwrap();

        match result {
            ColumnarValue::Array(v) => assert!(!v.is_empty()),
            _ => panic!(),
        }
    }

    #[test]
    fn test_column_expr_data_type() {
        let schema = create_schema();
        let expr = ColumnExpr::new("a", 0);

        let result = expr.data_type(&schema).unwrap();
        let expected = DataType::Utf8;
        assert_eq!(result, expected);

        let expr = ColumnExpr::new("b", 4);
        let try_res = expr.data_type(&schema);
        assert!(try_res.is_err());
    }
}

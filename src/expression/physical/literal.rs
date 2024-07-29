use std::{any::Any, fmt::Display};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Schema},
};

use crate::{
    error::Result,
    expression::values::{ColumnarValue, ScalarValue},
};

use super::expr::PhysicalExpression;

/// Represents a literal value in the physical execution plan.
#[derive(Debug)]
pub struct LiteralExpr {
    value: ScalarValue,
}

impl LiteralExpr {
    /// Creates a new [`LiteralExpr`] instance.
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl PhysicalExpression for LiteralExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _schema: &Schema) -> Result<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _schema: &Schema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    fn eval(&self, _input: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }
}

impl Display for LiteralExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::{
        expression::{
            physical::expr::PhysicalExpression,
            values::{ColumnarValue, ScalarValue},
        },
        tests::{create_record_batch, create_schema},
    };

    use super::LiteralExpr;

    #[test]
    fn test_literal_expr_eval() {
        let input = create_record_batch();
        let expr = LiteralExpr::new(ScalarValue::Utf8(Some("a".to_string())));

        let result = expr
            .eval(&input)
            .unwrap()
            .into_array(input.num_rows())
            .unwrap();
        let expected = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string())))
            .into_array(input.num_rows())
            .unwrap();
        assert_eq!(&result, &expected);
    }

    #[test]
    fn test_literal_expr_data_type() {
        let schema = create_schema();
        let expr = LiteralExpr::new(ScalarValue::Utf8(Some("a".to_string())));

        let result = expr.data_type(&schema).unwrap();
        assert_eq!(result, DataType::Utf8);
    }
}

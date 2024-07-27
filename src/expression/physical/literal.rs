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

#[derive(Debug)]
pub struct LiteralExpr {
    value: ScalarValue,
}

impl LiteralExpr {
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

    fn eval(&self, _input: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }
}

impl Display for LiteralExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

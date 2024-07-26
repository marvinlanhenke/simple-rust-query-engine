use std::{
    any::Any,
    fmt::{Debug, Display},
};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Schema},
};

use crate::{error::Result, expression::scalar::ColumnarValue};

pub trait PhysicalExpression: Display + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn data_type(&self, schema: &Schema) -> Result<DataType>;

    fn eval(&self, input: &RecordBatch) -> Result<ColumnarValue>;
}

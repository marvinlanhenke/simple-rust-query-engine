use std::{
    any::Any,
    fmt::{Debug, Display},
};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Schema},
};

use crate::error::Result;

pub trait PhysicalExpression: Display + Debug {
    fn as_any(&self) -> &dyn Any;

    fn data_type(&self, schema: &Schema) -> Result<DataType>;

    fn eval(&self, input: &RecordBatch) -> Result<()>;
}

use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{
    array::{ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray},
    compute,
    datatypes::{
        ArrowNativeType, DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{physical::expr::PhysicalExpression, values::ScalarValue},
};

use super::{Accumulator, AggregateExpr};

/// Represents a max aggregate expression.
#[derive(Debug)]
pub struct MaxExpr {
    /// The input expression used by the accumulator.
    expression: Arc<dyn PhysicalExpression>,
    /// The input datatype.
    data_type: DataType,
}

impl MaxExpr {
    /// Creates a new [`MaxExpr`] instance.
    pub fn new(expression: Arc<dyn PhysicalExpression>, data_type: DataType) -> Self {
        Self {
            expression,
            data_type,
        }
    }

    /// Returns the function's name including it's expressions.
    pub fn name(&self) -> String {
        format!("MAX({})", self.expression)
    }
}

/// Creates an type specific max accumulator.
macro_rules! make_max_accumulator {
    ($t:ty, $dt:expr) => {
        Ok(Box::new(MaxAccumulator::<$t>::try_new($dt.clone())?))
    };
}

impl AggregateExpr for MaxExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(self.name(), self.data_type.clone(), true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(self.name(), self.data_type.clone(), true)])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpression>> {
        vec![self.expression.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        match self.data_type {
            DataType::Int16 => make_max_accumulator!(Int16Type, self.data_type),
            DataType::Int32 => make_max_accumulator!(Int32Type, self.data_type),
            DataType::Int64 => make_max_accumulator!(Int64Type, self.data_type),
            DataType::UInt16 => make_max_accumulator!(UInt16Type, self.data_type),
            DataType::UInt32 => make_max_accumulator!(UInt32Type, self.data_type),
            DataType::UInt64 => make_max_accumulator!(UInt64Type, self.data_type),
            DataType::Float32 => make_max_accumulator!(Float32Type, self.data_type),
            DataType::Float64 => make_max_accumulator!(Float64Type, self.data_type),
            _ => Err(Error::InvalidOperation {
                message: format!("Sum not supported for datatype {}", self.data_type),
                location: location!(),
            }),
        }
    }
}

/// Represents the accumulator for the max expression.
pub struct MaxAccumulator<T: ArrowNumericType> {
    /// The current max.
    max: Option<T::Native>,
    /// The input datatype.
    data_type: DataType,
}

impl<T: ArrowNumericType> Debug for MaxAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MaxAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> MaxAccumulator<T> {
    /// Attempts to create a new [`MaxAccumulator`] instance.
    pub fn try_new(data_type: DataType) -> Result<Self> {
        Ok(Self {
            max: None,
            data_type,
        })
    }
}

impl<T: ArrowNumericType> Accumulator for MaxAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.max, &self.data_type)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0].as_primitive::<T>();
        if let Some(new_max) = compute::max(array) {
            let curr_max = self.max.get_or_insert(T::Native::usize_as(0));
            if new_max.is_gt(*curr_max) {
                *curr_max = new_max
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::{
        expression::{
            physical::{aggregate::AggregateExpr, column::ColumnExpr},
            values::ScalarValue,
        },
        tests::create_record_batch,
    };

    use super::MaxExpr;

    #[test]
    fn test_max_accumulator() {
        let expr = MaxExpr::new(Arc::new(ColumnExpr::new("a", 0)), DataType::Int64);
        let batch = create_record_batch();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(&[batch.column(1).clone()]).unwrap();
        let result = accum.eval().unwrap();
        let expected = ScalarValue::Int64(Some(2));

        assert_eq!(result, expected);
    }
}

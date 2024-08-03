use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{
    array::{ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray},
    compute,
    datatypes::{
        ArrowNativeType, DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_array::PrimitiveArray;
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{physical::expr::PhysicalExpression, values::ScalarValue},
};

use super::{Accumulator, AggregateExpr, GroupAccumulator};

/// Represents a sum aggregate expression.
#[derive(Debug)]
pub struct SumExpr {
    /// The input expression used by the accumulator.
    expression: Arc<dyn PhysicalExpression>,
    /// The input datatype.
    data_type: DataType,
}

impl SumExpr {
    /// Creates a new [`SumExpr`] instance.
    pub fn new(expression: Arc<dyn PhysicalExpression>, data_type: DataType) -> Self {
        Self {
            expression,
            data_type,
        }
    }

    /// Returns the function's name including it's expressions.
    pub fn name(&self) -> String {
        format!("SUM({})", self.expression)
    }
}

/// Creates an type specific accumulator.
macro_rules! make_accumulator {
    ($accu:ident, $t:ty, $dt:expr) => {
        Ok(Box::new($accu::<$t>::new($dt.clone())))
    };
}

impl AggregateExpr for SumExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(self.name(), DataType::Int64, true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(self.name(), DataType::Int64, true)])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpression>> {
        vec![self.expression.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        match self.data_type {
            DataType::Int16 => make_accumulator!(SumAccumulator, Int16Type, self.data_type),
            DataType::Int32 => make_accumulator!(SumAccumulator, Int32Type, self.data_type),
            DataType::Int64 => make_accumulator!(SumAccumulator, Int64Type, self.data_type),
            DataType::UInt16 => make_accumulator!(SumAccumulator, UInt16Type, self.data_type),
            DataType::UInt32 => make_accumulator!(SumAccumulator, UInt32Type, self.data_type),
            DataType::UInt64 => make_accumulator!(SumAccumulator, UInt64Type, self.data_type),
            DataType::Float32 => make_accumulator!(SumAccumulator, Float32Type, self.data_type),
            DataType::Float64 => make_accumulator!(SumAccumulator, Float64Type, self.data_type),
            _ => Err(Error::InvalidOperation {
                message: format!("Sum not supported for datatype {}", self.data_type),
                location: location!(),
            }),
        }
    }

    fn group_accumulator_supported(&self) -> bool {
        true
    }

    fn create_group_accumulator(&self) -> Result<Box<dyn GroupAccumulator>> {
        match self.data_type {
            DataType::Int16 => make_accumulator!(SumGroupAccumulator, Int16Type, self.data_type),
            DataType::Int32 => make_accumulator!(SumGroupAccumulator, Int32Type, self.data_type),
            DataType::Int64 => make_accumulator!(SumGroupAccumulator, Int64Type, self.data_type),
            DataType::UInt16 => make_accumulator!(SumGroupAccumulator, UInt16Type, self.data_type),
            DataType::UInt32 => make_accumulator!(SumGroupAccumulator, UInt32Type, self.data_type),
            DataType::UInt64 => make_accumulator!(SumGroupAccumulator, UInt64Type, self.data_type),
            DataType::Float32 => {
                make_accumulator!(SumGroupAccumulator, Float32Type, self.data_type)
            }
            DataType::Float64 => {
                make_accumulator!(SumGroupAccumulator, Float64Type, self.data_type)
            }
            _ => Err(Error::InvalidOperation {
                message: format!("Sum not supported for datatype {}", self.data_type),
                location: location!(),
            }),
        }
    }
}

/// Represents the accumulator for the sum expression.
pub struct SumAccumulator<T: ArrowNumericType> {
    /// The current sum.
    sum: Option<T::Native>,
    /// The input datatype.
    data_type: DataType,
}

impl<T: ArrowNumericType> Debug for SumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> SumAccumulator<T> {
    /// Create a new [`SumAccumulator`] instance.
    pub fn new(data_type: DataType) -> Self {
        Self {
            sum: None,
            data_type,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for SumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.sum, &self.data_type)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");

        let array = values[0].as_primitive::<T>();
        if let Some(delta) = compute::sum(array) {
            let s = self.sum.get_or_insert(T::Native::usize_as(0));
            *s = s.add_wrapping(delta);
        }
        Ok(())
    }
}

/// Represents a sum aggregate expression with associated groupings.
pub struct SumGroupAccumulator<T: ArrowNumericType> {
    /// The current sum per group index.
    sums: Vec<T::Native>,
    /// The starting value for a new group.
    starting_value: T::Native,
    /// The input datatype.
    data_type: DataType,
}

impl<T: ArrowNumericType> Debug for SumGroupAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SumGroupAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> SumGroupAccumulator<T> {
    /// Create a new [`SumGroupAccumulator`] instance.
    pub fn new(data_type: DataType) -> Self {
        Self {
            sums: vec![],
            starting_value: Default::default(),
            data_type,
        }
    }
}

impl<T: ArrowNumericType> GroupAccumulator for SumGroupAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ArrayRef> {
        let sums = self.sums.clone();
        let array =
            PrimitiveArray::<T>::new(sums.into(), None).with_data_type(self.data_type.clone());
        Ok(Arc::new(array))
    }

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");

        let array = values[0].as_primitive::<T>();

        self.sums.resize(total_num_groups, self.starting_value);

        for (idx, value) in group_indices.iter().zip(array.iter()) {
            if let Some(v) = value {
                self.sums[*idx] = self.sums[*idx].add_wrapping(v);
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
            physical::{
                aggregate::{sum::SumExpr, AggregateExpr},
                column::ColumnExpr,
            },
            values::ScalarValue,
        },
        tests::{create_record_batch, create_record_batch_with_nulls},
    };

    #[test]
    fn test_sum_group_accumulator() {
        let expr = SumExpr::new(Arc::new(ColumnExpr::new("c2", 1)), DataType::Int64);
        let batch = create_record_batch_with_nulls();
        let mut accum = expr.create_group_accumulator().unwrap();

        let values = &[batch.column(1).clone()];
        let group_indices = &[0, 1, 0];
        let total_num_groups = 2;
        accum
            .update_batch(values, group_indices, total_num_groups)
            .unwrap();
        let result = accum.eval().unwrap();
        let expected = &[1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(result.to_data().buffers()[0].as_slice(), expected);
    }

    #[test]
    fn test_sum_accumulator() {
        let expr = SumExpr::new(Arc::new(ColumnExpr::new("a", 0)), DataType::Int64);
        let batch = create_record_batch();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(&[batch.column(1).clone()]).unwrap();
        let result = accum.eval().unwrap();
        let expected = ScalarValue::Int64(Some(3));

        assert_eq!(result, expected);
    }
}

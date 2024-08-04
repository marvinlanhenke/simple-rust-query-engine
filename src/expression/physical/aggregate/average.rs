use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray},
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

/// Represents an avg aggregate expression.
#[derive(Debug)]
pub struct AvgExpr {
    /// The input expression used by the accumulator.
    expression: Arc<dyn PhysicalExpression>,
    /// The input datatype.
    input_data_type: DataType,
    /// The result datatype.
    output_data_tye: DataType,
}

impl AvgExpr {
    /// Creates a new [`AvgExpr`] instance.
    pub fn new(expression: Arc<dyn PhysicalExpression>, input_data_type: DataType) -> Self {
        Self {
            expression,
            input_data_type,
            output_data_tye: DataType::Float64,
        }
    }

    /// Returns the function's name including it's expressions.
    pub fn name(&self) -> String {
        format!("AVG({})", self.expression)
    }
}

/// Creates an type specific accumulator.
macro_rules! make_accumulator {
    ($accu:ident, $t:ty, $dt:expr) => {
        Ok(Box::new($accu::<$t>::new($dt.clone())))
    };
}

impl AggregateExpr for AvgExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(self.name(), self.output_data_tye.clone(), true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(format!("{}[count]", self.name()), DataType::Int64, true),
            Field::new(
                format!("{}[sum]", self.name()),
                self.input_data_type.clone(),
                true,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpression>> {
        vec![self.expression.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        match self.input_data_type {
            DataType::Int16 => make_accumulator!(AvgAccumulator, Int16Type, self.input_data_type),
            DataType::Int32 => make_accumulator!(AvgAccumulator, Int32Type, self.input_data_type),
            DataType::Int64 => make_accumulator!(AvgAccumulator, Int64Type, self.input_data_type),
            DataType::UInt16 => make_accumulator!(AvgAccumulator, UInt16Type, self.input_data_type),
            DataType::UInt32 => make_accumulator!(AvgAccumulator, UInt32Type, self.input_data_type),
            DataType::UInt64 => make_accumulator!(AvgAccumulator, UInt64Type, self.input_data_type),
            DataType::Float32 => {
                make_accumulator!(AvgAccumulator, Float32Type, self.input_data_type)
            }
            DataType::Float64 => {
                make_accumulator!(AvgAccumulator, Float64Type, self.input_data_type)
            }
            _ => Err(Error::InvalidOperation {
                message: format!("Sum not supported for datatype {}", self.input_data_type),
                location: location!(),
            }),
        }
    }

    fn group_accumulator_supported(&self) -> bool {
        true
    }

    fn create_group_accumulator(&self) -> Result<Box<dyn GroupAccumulator>> {
        if !self.group_accumulator_supported() {
            return Err(Error::InvalidOperation {
                message: "GroupAccumulator is not supported".to_string(),
                location: location!(),
            });
        }

        match self.input_data_type {
            DataType::Int16 => {
                make_accumulator!(AvgGroupAccumulator, Int16Type, self.input_data_type)
            }
            DataType::Int32 => {
                make_accumulator!(AvgGroupAccumulator, Int32Type, self.input_data_type)
            }
            DataType::Int64 => {
                make_accumulator!(AvgGroupAccumulator, Int64Type, self.input_data_type)
            }
            DataType::UInt16 => {
                make_accumulator!(AvgGroupAccumulator, UInt16Type, self.input_data_type)
            }
            DataType::UInt32 => {
                make_accumulator!(AvgGroupAccumulator, UInt32Type, self.input_data_type)
            }
            DataType::UInt64 => {
                make_accumulator!(AvgGroupAccumulator, UInt64Type, self.input_data_type)
            }
            DataType::Float32 => {
                make_accumulator!(AvgGroupAccumulator, Float32Type, self.input_data_type)
            }
            DataType::Float64 => {
                make_accumulator!(AvgGroupAccumulator, Float64Type, self.input_data_type)
            }
            _ => Err(Error::InvalidOperation {
                message: format!("Sum not supported for datatype {}", self.input_data_type),
                location: location!(),
            }),
        }
    }
}

/// Represents the accumulator for the avg expression.
pub struct AvgAccumulator<T: ArrowNumericType> {
    /// The current count.
    count: i64,
    /// The current sum.
    sum: Option<T::Native>,
    /// The input datatype.
    data_type: DataType,
}

impl<T: ArrowNumericType> Debug for AvgAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AvgAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> AvgAccumulator<T> {
    pub fn new(data_type: DataType) -> Self {
        Self {
            count: 0,
            sum: None,
            data_type,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for AvgAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Int64(Some(self.count)),
            ScalarValue::new_primitive::<T>(self.sum, &self.data_type)?,
        ])
    }

    fn eval(&mut self) -> Result<ScalarValue> {
        let avg = self.sum.map(|v| v.as_usize() as f64 / self.count as f64);
        Ok(ScalarValue::Float64(avg))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0].as_primitive::<T>();
        let null_count = array.logical_nulls().map_or(0, |x| x.null_count());
        self.count += (array.len() - null_count) as i64;

        if let Some(delta) = compute::sum(array) {
            let s = self.sum.get_or_insert(T::Native::usize_as(0));
            *s = s.add_wrapping(delta);
        }
        Ok(())
    }
}

/// Represents an avg aggregate expression with associated groupings.
pub struct AvgGroupAccumulator<T: ArrowNumericType> {
    /// The current count per group.
    counts: Vec<i64>,
    /// The current sum per group.
    sums: Vec<T::Native>,
    /// The starting value for a new sum group.
    starting_value: T::Native,
    /// The input datatype.
    data_type: DataType,
}

impl<T: ArrowNumericType> Debug for AvgGroupAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AvgGroupAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> AvgGroupAccumulator<T> {
    /// Creates a new [`AvgGroupAccumulator`] instance.
    pub fn new(data_type: DataType) -> Self {
        Self {
            counts: vec![],
            sums: vec![],
            starting_value: Default::default(),
            data_type,
        }
    }
}

impl<T: ArrowNumericType> GroupAccumulator for AvgGroupAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ArrayRef> {
        assert_eq!(self.counts.len(), self.sums.len());

        let averages: Vec<_> = self
            .sums
            .iter()
            .zip(self.counts.iter())
            .map(|(sum, count)| sum.as_usize() as f64 / *count as f64)
            .collect();
        let array = PrimitiveArray::<Float64Type>::new(averages.into(), None)
            .with_data_type(DataType::Float64);
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

        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, self.starting_value);

        for (idx, value) in group_indices.iter().zip(array.iter()) {
            if let Some(v) = value {
                self.sums[*idx] = self.sums[*idx].add_wrapping(v);
                self.counts[*idx] += 1;
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
        tests::{create_record_batch, create_record_batch_with_nulls},
    };

    use super::AvgExpr;

    #[test]
    fn test_avg_group_accumulator() {
        let expr = AvgExpr::new(Arc::new(ColumnExpr::new("c2", 1)), DataType::Int64);
        let batch = create_record_batch_with_nulls();
        let mut accum = expr.create_group_accumulator().unwrap();

        let values = &[batch.column(1).clone()];
        let group_indices = &[0, 1, 0];
        let total_num_groups = 2;
        accum
            .update_batch(values, group_indices, total_num_groups)
            .unwrap();
        let result = accum.eval().unwrap();
        // [1.0, 2.0]
        let expected = &[0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64];
        assert_eq!(result.to_data().buffers()[0].as_slice(), expected);
    }

    #[test]
    fn test_avg_accumulator() {
        let expr = AvgExpr::new(Arc::new(ColumnExpr::new("c2", 1)), DataType::Int64);
        let batch = create_record_batch();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(&[batch.column(1).clone()]).unwrap();
        let result = accum.eval().unwrap();
        let expected = ScalarValue::Float64(Some(1.5));
        assert_eq!(result, expected);
    }
}

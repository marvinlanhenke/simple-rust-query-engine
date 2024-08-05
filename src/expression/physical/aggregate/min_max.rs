use std::{any::Any, collections::HashSet, fmt::Debug, sync::Arc};

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

/// Creates an type specific accumulator.
macro_rules! make_accumulator {
    ($accu:ident, $t:ty, $dt:expr) => {
        Ok(Box::new($accu::<$t>::new($dt.clone())))
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
            DataType::Int16 => make_accumulator!(MaxAccumulator, Int16Type, self.data_type),
            DataType::Int32 => make_accumulator!(MaxAccumulator, Int32Type, self.data_type),
            DataType::Int64 => make_accumulator!(MaxAccumulator, Int64Type, self.data_type),
            DataType::UInt16 => make_accumulator!(MaxAccumulator, UInt16Type, self.data_type),
            DataType::UInt32 => make_accumulator!(MaxAccumulator, UInt32Type, self.data_type),
            DataType::UInt64 => make_accumulator!(MaxAccumulator, UInt64Type, self.data_type),
            DataType::Float32 => make_accumulator!(MaxAccumulator, Float32Type, self.data_type),
            DataType::Float64 => make_accumulator!(MaxAccumulator, Float64Type, self.data_type),
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
        if !self.group_accumulator_supported() {
            return Err(Error::InvalidOperation {
                message: "GroupAccumulator is not supported".to_string(),
                location: location!(),
            });
        }

        match self.data_type {
            DataType::Int16 => {
                make_accumulator!(MaxGroupAccumulator, Int16Type, self.data_type)
            }
            DataType::Int32 => {
                make_accumulator!(MaxGroupAccumulator, Int32Type, self.data_type)
            }
            DataType::Int64 => {
                make_accumulator!(MaxGroupAccumulator, Int64Type, self.data_type)
            }
            DataType::UInt16 => {
                make_accumulator!(MaxGroupAccumulator, UInt16Type, self.data_type)
            }
            DataType::UInt32 => {
                make_accumulator!(MaxGroupAccumulator, UInt32Type, self.data_type)
            }
            DataType::UInt64 => {
                make_accumulator!(MaxGroupAccumulator, UInt64Type, self.data_type)
            }
            DataType::Float32 => {
                make_accumulator!(MaxGroupAccumulator, Float32Type, self.data_type)
            }
            DataType::Float64 => {
                make_accumulator!(MaxGroupAccumulator, Float64Type, self.data_type)
            }
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
    /// Creates a new [`MaxAccumulator`] instance.
    pub fn new(data_type: DataType) -> Self {
        Self {
            max: None,
            data_type,
        }
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

/// Represents a max aggregate expression with associated groupings.
pub struct MaxGroupAccumulator<T: ArrowNumericType> {
    /// The current max values per group index.
    maxes: Vec<T::Native>,
    /// The starting value for a new group.
    starting_value: T::Native,
    /// The input datatype.
    data_type: DataType,
}

impl<T: ArrowNumericType> Debug for MaxGroupAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MaxGroupAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> MaxGroupAccumulator<T> {
    pub fn new(data_type: DataType) -> Self {
        Self {
            maxes: vec![],
            starting_value: Default::default(),
            data_type,
        }
    }
}

impl<T: ArrowNumericType> GroupAccumulator for MaxGroupAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ArrayRef> {
        let maxes = self.maxes.clone();
        let array =
            PrimitiveArray::<T>::new(maxes.into(), None).with_data_type(self.data_type.clone());
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

        self.maxes.resize(total_num_groups, self.starting_value);

        let mut seen_idx: HashSet<usize> = HashSet::new();
        for (idx, value) in group_indices.iter().zip(array.iter()) {
            if let Some(v) = value {
                match seen_idx.insert(*idx) {
                    true => self.maxes[*idx] = v,
                    false if self.maxes[*idx] < v => self.maxes[*idx] = v,
                    _ => continue,
                }
            }
        }

        Ok(())
    }
}

/// Represents a min aggregate expression.
#[derive(Debug)]
pub struct MinExpr {
    /// The input expression used by the accumulator.
    expression: Arc<dyn PhysicalExpression>,
    /// The input datatype.
    data_type: DataType,
}

impl MinExpr {
    /// Creates a new [`MinExpr`] instance.
    pub fn new(expression: Arc<dyn PhysicalExpression>, data_type: DataType) -> Self {
        Self {
            expression,
            data_type,
        }
    }

    /// Returns the function's name including it's expressions.
    pub fn name(&self) -> String {
        format!("MIN({})", self.expression)
    }
}

impl AggregateExpr for MinExpr {
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
            DataType::Int16 => {
                make_accumulator!(MinAccumulator, Int16Type, self.data_type)
            }
            DataType::Int32 => {
                make_accumulator!(MinAccumulator, Int32Type, self.data_type)
            }
            DataType::Int64 => {
                make_accumulator!(MinAccumulator, Int64Type, self.data_type)
            }
            DataType::UInt16 => {
                make_accumulator!(MinAccumulator, UInt16Type, self.data_type)
            }
            DataType::UInt32 => {
                make_accumulator!(MinAccumulator, UInt32Type, self.data_type)
            }
            DataType::UInt64 => {
                make_accumulator!(MinAccumulator, UInt64Type, self.data_type)
            }
            DataType::Float32 => {
                make_accumulator!(MinAccumulator, Float32Type, self.data_type)
            }
            DataType::Float64 => {
                make_accumulator!(MinAccumulator, Float64Type, self.data_type)
            }
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
        if !self.group_accumulator_supported() {
            return Err(Error::InvalidOperation {
                message: "GroupAccumulator is not supported".to_string(),
                location: location!(),
            });
        }

        match self.data_type {
            DataType::Int16 => {
                make_accumulator!(MinGroupAccumulator, Int16Type, self.data_type)
            }
            DataType::Int32 => {
                make_accumulator!(MinGroupAccumulator, Int32Type, self.data_type)
            }
            DataType::Int64 => {
                make_accumulator!(MinGroupAccumulator, Int64Type, self.data_type)
            }
            DataType::UInt16 => {
                make_accumulator!(MinGroupAccumulator, UInt16Type, self.data_type)
            }
            DataType::UInt32 => {
                make_accumulator!(MinGroupAccumulator, UInt32Type, self.data_type)
            }
            DataType::UInt64 => {
                make_accumulator!(MinGroupAccumulator, UInt64Type, self.data_type)
            }
            DataType::Float32 => {
                make_accumulator!(MinGroupAccumulator, Float32Type, self.data_type)
            }
            DataType::Float64 => {
                make_accumulator!(MinGroupAccumulator, Float64Type, self.data_type)
            }
            _ => Err(Error::InvalidOperation {
                message: format!("Sum not supported for datatype {}", self.data_type),
                location: location!(),
            }),
        }
    }
}

/// Represents the accumulator for the min expression.
pub struct MinAccumulator<T: ArrowNumericType> {
    /// The current min.
    min: Option<T::Native>,
    /// The input datatype.
    data_type: DataType,
}

impl<T: ArrowNumericType> Debug for MinAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> MinAccumulator<T> {
    /// Creates a new [`MinAccumulator`] instance.
    pub fn new(data_type: DataType) -> Self {
        Self {
            min: None,
            data_type,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for MinAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.min, &self.data_type)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0].as_primitive::<T>();
        if let Some(new_min) = compute::min(array) {
            let curr_min = self.min.get_or_insert(T::Native::usize_as(0));
            if new_min.is_gt(*curr_min) {
                *curr_min = new_min
            }
        }
        Ok(())
    }
}

/// Represents a min aggregate expression with associated groupings.
pub struct MinGroupAccumulator<T: ArrowNumericType> {
    /// The current min values per group index.
    mins: Vec<T::Native>,
    /// The starting value for a new group.
    starting_value: T::Native,
    /// The input datatype.
    data_type: DataType,
}

impl<T: ArrowNumericType> Debug for MinGroupAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinGroupAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> MinGroupAccumulator<T> {
    pub fn new(data_type: DataType) -> Self {
        Self {
            mins: vec![],
            starting_value: Default::default(),
            data_type,
        }
    }
}

impl<T: ArrowNumericType> GroupAccumulator for MinGroupAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ArrayRef> {
        let mins = self.mins.clone();
        let array =
            PrimitiveArray::<T>::new(mins.into(), None).with_data_type(self.data_type.clone());
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

        self.mins.resize(total_num_groups, self.starting_value);

        let mut seen_idx: HashSet<usize> = HashSet::new();
        for (idx, value) in group_indices.iter().zip(array.iter()) {
            if let Some(v) = value {
                match seen_idx.insert(*idx) {
                    true => self.mins[*idx] = v,
                    false if self.mins[*idx] > v => self.mins[*idx] = v,
                    _ => continue,
                }
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
                aggregate::{min_max::MinExpr, AggregateExpr},
                column::ColumnExpr,
            },
            values::ScalarValue,
        },
        tests::{create_record_batch, create_record_batch_with_nulls},
    };

    use super::MaxExpr;

    #[test]
    fn test_max_group_accumulator() {
        let expr = MaxExpr::new(Arc::new(ColumnExpr::new("c2", 1)), DataType::Int64);
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
    fn test_max_accumulator() {
        let expr = MaxExpr::new(Arc::new(ColumnExpr::new("a", 0)), DataType::Int64);
        let batch = create_record_batch();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(&[batch.column(1).clone()]).unwrap();
        let result = accum.eval().unwrap();
        let expected = ScalarValue::Int64(Some(2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_min_group_accumulator() {
        let expr = MinExpr::new(Arc::new(ColumnExpr::new("c2", 1)), DataType::Int64);
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
    fn test_min_accumulator() {
        let expr = MinExpr::new(Arc::new(ColumnExpr::new("a", 0)), DataType::Int64);
        let batch = create_record_batch();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(&[batch.column(1).clone()]).unwrap();
        let result = accum.eval().unwrap();
        let expected = ScalarValue::Int64(Some(1));

        assert_eq!(result, expected);
    }
}

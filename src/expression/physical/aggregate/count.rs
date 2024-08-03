use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, PrimitiveArray},
    datatypes::{DataType, Field, Int64Type},
};

use crate::{
    error::Result,
    expression::{physical::expr::PhysicalExpression, values::ScalarValue},
};

use super::{Accumulator, AggregateExpr, GroupAccumulator};

/// Represents a count aggregate expression.
#[derive(Debug)]
pub struct CountExpr {
    /// The input expression used by the accumulator.
    expression: Arc<dyn PhysicalExpression>,
}

impl CountExpr {
    /// Creates a new [`CountExpr`] instance.
    pub fn new(expression: Arc<dyn PhysicalExpression>) -> Self {
        Self { expression }
    }

    /// Returns the function's name including it's expressions.
    pub fn name(&self) -> String {
        format!("COUNT({})", self.expression)
    }
}

impl AggregateExpr for CountExpr {
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
        Ok(Box::new(CountAccumulator::new()))
    }

    fn group_accumulator_supported(&self) -> bool {
        true
    }

    fn create_group_accumulator(&self) -> Result<Box<dyn GroupAccumulator>> {
        Ok(Box::new(CountGroupAccumulator::new()))
    }
}

/// Represents the accumulator for the count expression.
#[derive(Debug, Default)]
pub struct CountAccumulator {
    /// The current count of non-null values.
    count: i64,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for CountAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        let null_count = array.logical_nulls().map_or(0, |x| x.null_count());
        self.count += (array.len() - null_count) as i64;
        Ok(())
    }
}

/// Represents a count aggregate expression with associated groupings.
///
/// This accumulator tracks the frequency of each index within the group indices of a [`RecordBatch`].
/// Each index represents a distinct entry, managed by [`GroupValue`], which hashes (key) each grouping row
/// and stores it in a `HashMap` with a unique identifier (value). The occurence of each unique entry is recorded
/// to construct the `group_indices` vector for the current batch. For each group index, the accumulator
/// increments the associated count by one, provided the index is not null.
///
/// # Example
///
///   ┌─────┐                    ┌───────────┐
///   │ A:0 │    ┌─┬─┬─┬─┬───┐   │  counts   │
///   ├─────┤    │0│0│0│1│...│   │ ┌───┬───┐ │
///   ├─────┤    └─┴─┴─┴─┴───┘   │ │ 3 │ 1 │ │
///   │ B:1 │    group indices   │ └───┴───┘ │
///   └─────┘                    │Accumulator│
/// group values                 └───────────┘
///
#[derive(Debug, Default)]
pub struct CountGroupAccumulator {
    /// The current count per group index.
    counts: Vec<i64>,
}

impl CountGroupAccumulator {
    /// Creates a new [`CountGroupAccumulator`] instance.
    pub fn new() -> Self {
        Self::default()
    }
}

impl GroupAccumulator for CountGroupAccumulator {
    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ArrayRef> {
        let counts = self.counts.clone();
        let array = PrimitiveArray::<Int64Type>::new(counts.into(), None);
        Ok(Arc::new(array))
    }

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.counts.resize(total_num_groups, 0);
        let array = &values[0];

        // increase the count by one for each group index
        // provided the batch value at this index is not null
        match array.logical_nulls() {
            Some(nb) => {
                for (idx, is_valid) in group_indices.iter().zip(nb.iter()) {
                    if is_valid {
                        self.counts[*idx] += 1
                    }
                }
            }
            None => {
                for idx in group_indices {
                    self.counts[*idx] += 1;
                }
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        expression::{
            physical::{aggregate::AggregateExpr, column::ColumnExpr},
            values::ScalarValue,
        },
        tests::create_record_batch_with_nulls,
    };

    use super::CountExpr;

    #[test]
    fn test_count_group_accumulator() {
        let expr = CountExpr::new(Arc::new(ColumnExpr::new("c1", 0)));
        let batch = create_record_batch_with_nulls();
        let mut accum = expr.create_group_accumulator().unwrap();

        let values = batch.columns();
        let group_indices = &[0, 0, 1];
        let total_num_groups = 2;
        accum
            .update_batch(values, group_indices, total_num_groups)
            .unwrap();

        let result = accum.eval().unwrap();
        let expected = &[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(result.len(), 2);
        assert_eq!(result.to_data().buffers()[0].as_slice(), expected);
    }

    #[test]
    fn test_count_accumulator() {
        let expr = CountExpr::new(Arc::new(ColumnExpr::new("c1", 0)));
        let batch = create_record_batch_with_nulls();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(batch.columns()).unwrap();
        let result = accum.eval().unwrap();
        let expected = ScalarValue::Int64(Some(2));

        assert_eq!(result, expected);
    }
}

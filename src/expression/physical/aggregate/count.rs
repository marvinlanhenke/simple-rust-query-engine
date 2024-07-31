use std::{any::Any, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};

use crate::{
    error::Result,
    expression::{physical::expr::PhysicalExpression, values::ScalarValue},
};

use super::{Accumulator, AggregateExpr};

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        expression::{
            physical::{aggregate::AggregateExpr, column::ColumnExpr},
            values::ScalarValue,
        },
        tests::create_record_batch,
    };

    use super::CountExpr;

    #[test]
    fn test_count_accumulator() {
        let expr = CountExpr::new(Arc::new(ColumnExpr::new("a", 0)));
        let batch = create_record_batch();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(batch.columns()).unwrap();
        let result = accum.eval().unwrap();
        let expected = ScalarValue::Int64(Some(2));

        assert_eq!(result, expected);
    }
}

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
    /// The input expressions used by the accumulator.
    expressions: Vec<Arc<dyn PhysicalExpression>>,
}

impl CountExpr {
    /// Creates a new [`CountExpr`] instance.
    pub fn new(expression: Arc<dyn PhysicalExpression>) -> Self {
        Self {
            expressions: vec![expression],
        }
    }

    /// Returns the function's name including it's expressions.
    pub fn name(&self) -> String {
        let expr_str = self
            .expressions
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("COUNT({})", expr_str)
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
        self.expressions.clone()
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
        Ok(vec![ScalarValue::Int64(Some(self.count))])
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
mod tests {}

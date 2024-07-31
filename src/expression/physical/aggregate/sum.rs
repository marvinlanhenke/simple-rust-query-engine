use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, ArrowNativeTypeOp, Int64Array},
    compute,
    datatypes::{DataType, Field},
};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{physical::expr::PhysicalExpression, values::ScalarValue},
};

use super::{Accumulator, AggregateExpr};

#[derive(Debug)]
pub struct SumExpr {
    expressions: Vec<Arc<dyn PhysicalExpression>>,
}

impl SumExpr {
    /// Creates a new [`SumExpr`] instance.
    pub fn new(expressions: Vec<Arc<dyn PhysicalExpression>>) -> Self {
        Self { expressions }
    }

    /// Returns the function's name including it's expressions.
    pub fn name(&self) -> String {
        let expr_str = self
            .expressions
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("SUM({})", expr_str)
    }
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
        self.expressions.clone()
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumAccumulator::new()))
    }
}

#[derive(Debug, Default)]
pub struct SumAccumulator {
    sum: Option<i64>,
}

impl SumAccumulator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for SumAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.eval()?])
    }

    fn eval(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(self.sum))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Arrow {
                message: "Failed to downcast array".to_string(),
                location: location!(),
            })?;
        if let Some(delta) = compute::sum(array) {
            let s = self.sum.get_or_insert(0);
            *s = s.add_checked(delta)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        expression::{
            physical::{
                aggregate::{sum::SumExpr, AggregateExpr},
                column::ColumnExpr,
            },
            values::ScalarValue,
        },
        tests::create_record_batch,
    };

    #[test]
    fn test_sum_accumulator() {
        let expr = SumExpr::new(vec![Arc::new(ColumnExpr::new("a", 0))]);
        let batch = create_record_batch();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(&[batch.column(1).clone()]).unwrap();
        let result = accum.eval().unwrap();
        let expected = ScalarValue::Int64(Some(3));

        assert_eq!(result, expected);
    }
}

use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray},
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
    ($t:ty, $dt:expr) => {
        Ok(Box::new(AvgAccumulator::<$t>::new($dt.clone())))
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
            DataType::Int16 => make_accumulator!(Int16Type, self.input_data_type),
            DataType::Int32 => make_accumulator!(Int32Type, self.input_data_type),
            DataType::Int64 => make_accumulator!(Int64Type, self.input_data_type),
            DataType::UInt16 => make_accumulator!(UInt16Type, self.input_data_type),
            DataType::UInt32 => make_accumulator!(UInt32Type, self.input_data_type),
            DataType::UInt64 => make_accumulator!(UInt64Type, self.input_data_type),
            DataType::Float32 => make_accumulator!(Float32Type, self.input_data_type),
            DataType::Float64 => make_accumulator!(Float64Type, self.input_data_type),
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
        todo!()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::{
        expression::physical::{aggregate::AggregateExpr, column::ColumnExpr},
        tests::create_record_batch,
    };

    use super::AvgExpr;

    #[test]
    fn test_avg_accumulator() {
        let expr = AvgExpr::new(Arc::new(ColumnExpr::new("a", 0)), DataType::Int64);
        let batch = create_record_batch();
        let mut accum = expr.create_accumulator().unwrap();

        accum.update_batch(&[batch.column(1).clone()]).unwrap();
        let result = accum.eval().unwrap();
        println!("{result:?}");
    }
}

use std::{any::Any, fmt::Display, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanArray, Datum, RecordBatch},
    compute::{
        and_kleene,
        kernels::{
            cmp::{eq, gt, gt_eq, lt, lt_eq, neq},
            numeric::{add_wrapping, div, mul_wrapping, sub_wrapping},
        },
        or_kleene,
    },
    datatypes::{DataType, Schema},
    error::ArrowError,
};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{
        coercion::Signature,
        operator::Operator,
        values::{ColumnarValue, ScalarValue},
    },
};

use super::expr::PhysicalExpression;

#[derive(Debug)]
pub struct BinaryExpr {
    lhs: Arc<dyn PhysicalExpression>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpression>,
}

impl BinaryExpr {
    pub fn new(
        lhs: Arc<dyn PhysicalExpression>,
        op: Operator,
        rhs: Arc<dyn PhysicalExpression>,
    ) -> Self {
        Self { lhs, op, rhs }
    }

    fn apply_cmp(
        lhs: &ColumnarValue,
        rhs: &ColumnarValue,
        f: impl Fn(&dyn Datum, &dyn Datum) -> std::result::Result<BooleanArray, ArrowError>,
    ) -> Result<ColumnarValue> {
        Self::apply(lhs, rhs, |l, r| Ok(Arc::new(f(l, r)?)))
    }

    fn apply(
        lhs: &ColumnarValue,
        rhs: &ColumnarValue,
        f: impl Fn(&dyn Datum, &dyn Datum) -> std::result::Result<ArrayRef, ArrowError>,
    ) -> Result<ColumnarValue> {
        use ColumnarValue::*;

        match (&lhs, &rhs) {
            (Array(l), Array(r)) => Ok(Array(f(&l.as_ref(), &r.as_ref())?)),
            (Scalar(l), Array(r)) => Ok(Array(f(&l.to_scalar()?, &r.as_ref())?)),
            (Array(l), Scalar(r)) => Ok(Array(f(&l.as_ref(), &r.to_scalar()?)?)),
            (Scalar(l), Scalar(r)) => {
                let arr = f(&l.to_scalar()?, &r.to_scalar()?)?;
                let value = ScalarValue::try_from_array(arr.as_ref(), 0)?;
                Ok(Scalar(value))
            }
        }
    }
}

impl PhysicalExpression for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        let lhs = self.lhs.data_type(schema)?;
        let rhs = self.rhs.data_type(schema)?;
        Signature::get_result_type(&lhs, &self.op, &rhs)
    }

    fn eval(&self, input: &RecordBatch) -> Result<ColumnarValue> {
        use Operator::*;

        let lhs = self.lhs.eval(input)?;
        let rhs = self.rhs.eval(input)?;

        match self.op {
            Eq => Self::apply_cmp(&lhs, &rhs, eq),
            NotEq => Self::apply_cmp(&lhs, &rhs, neq),
            Lt => Self::apply_cmp(&lhs, &rhs, lt),
            LtEq => Self::apply_cmp(&lhs, &rhs, lt_eq),
            Gt => Self::apply_cmp(&lhs, &rhs, gt),
            GtEq => Self::apply_cmp(&lhs, &rhs, gt_eq),
            Plus => Self::apply(&lhs, &rhs, add_wrapping),
            Minus => Self::apply(&lhs, &rhs, sub_wrapping),
            Multiply => Self::apply(&lhs, &rhs, mul_wrapping),
            Divide => Self::apply(&lhs, &rhs, div),
            Or | And => {
                let (left, right) = (
                    lhs.into_array(input.num_rows())?,
                    rhs.into_array(input.num_rows())?,
                );

                if left.data_type() != &DataType::Boolean {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "Cannot evaluate binary expression {} with types {} and {}",
                            self.op,
                            left.data_type(),
                            right.data_type()
                        ),
                        location: location!(),
                    });
                }

                let left = left
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::Arrow {
                        message: "Failed to downcast array".to_string(),
                        location: location!(),
                    })?;
                let right = right
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::Arrow {
                        message: "Failed to downcast array".to_string(),
                        location: location!(),
                    })?;

                let array = if self.op == And {
                    and_kleene(left, right)?
                } else {
                    or_kleene(left, right)?
                };

                Ok(ColumnarValue::Array(Arc::new(array)))
            }
        }
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write_child(
            f: &mut std::fmt::Formatter<'_>,
            expr: &dyn PhysicalExpression,
        ) -> std::fmt::Result {
            if let Some(child) = expr.as_any().downcast_ref::<BinaryExpr>() {
                write!(f, "{}", child)
            } else {
                write!(f, "{}", expr)
            }
        }
        write_child(f, self.lhs.as_ref())?;
        write!(f, " {} ", self.op)?;
        write_child(f, self.rhs.as_ref())
    }
}

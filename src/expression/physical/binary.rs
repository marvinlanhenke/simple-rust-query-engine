use std::{any::Any, fmt::Display, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Schema},
};

use crate::{
    error::Result,
    expression::{coercion::Signature, operator::Operator, values::ColumnarValue},
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

    fn eval(&self, _input: &RecordBatch) -> Result<ColumnarValue> {
        todo!()
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

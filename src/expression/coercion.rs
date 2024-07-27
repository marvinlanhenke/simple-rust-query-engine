use arrow::{
    array::{new_empty_array, ArrayRef},
    compute::kernels::numeric::{add_wrapping, div, mul_wrapping, sub_wrapping},
    datatypes::DataType,
};
use snafu::location;

use crate::error::{Error, Result};

use super::operator::Operator;

#[derive(Debug)]
pub struct Signature {
    /// The left input's [`DataType`].
    lhs: DataType,
    /// The right input's [`DataType`].
    rhs: DataType,
    /// The return value's [`DataType`].
    ret: DataType,
}

impl Signature {
    fn try_new(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<Self> {
        use DataType::*;
        use Operator::*;

        let coercion_err = |lhs: &DataType, op: &Operator, rhs: &DataType| -> Error {
            Error::InvalidData {
                message: format!(
                    "Cannot infer datatype from operation {} {} {}",
                    lhs, op, rhs
                ),
                location: location!(),
            }
        };

        match op {
            Eq | NotEq | Lt | LtEq | Gt | GtEq => {
                if lhs != rhs {
                    return Err(coercion_err(lhs, op, rhs));
                }
                Ok(Self {
                    lhs: lhs.clone(),
                    rhs: lhs.clone(),
                    ret: DataType::Boolean,
                })
            }
            Or | And => {
                if !matches!((lhs, rhs), (Boolean | Null, Boolean | Null)) {
                    return Err(coercion_err(lhs, op, rhs));
                }
                Ok(Self {
                    lhs: Boolean,
                    rhs: Boolean,
                    ret: Boolean,
                })
            }
            Plus | Minus | Multiply | Divide => {
                let left = new_empty_array(lhs);
                let right = new_empty_array(rhs);
                let result: ArrayRef = match op {
                    Plus => add_wrapping(&left, &right)?,
                    Minus => sub_wrapping(&left, &right)?,
                    Multiply => mul_wrapping(&left, &right)?,
                    Divide => div(&left, &right)?,
                    _ => unreachable!(),
                };
                Ok(Self {
                    lhs: lhs.clone(),
                    rhs: rhs.clone(),
                    ret: result.data_type().clone(),
                })
            }
        }
    }

    pub fn get_result_type(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<DataType> {
        Self::try_new(lhs, op, rhs).map(|sig| sig.ret)
    }

    pub fn get_input_types(
        lhs: &DataType,
        op: &Operator,
        rhs: &DataType,
    ) -> Result<(DataType, DataType)> {
        Self::try_new(lhs, op, rhs).map(|sig| (sig.lhs, sig.rhs))
    }
}

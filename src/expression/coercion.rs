use std::sync::Arc;

use arrow::{
    array::new_empty_array,
    compute::kernels::numeric::{add_wrapping, div, mul_wrapping, sub_wrapping},
    datatypes::DataType,
};

use arrow_schema::Schema;
use snafu::location;

use crate::error::{Error, Result};

use super::{
    logical::{binary::Binary, expr::Expression},
    operator::Operator,
};

/// Represents the signature of an operation,
/// including the input and output data types.
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
    /// Attempts to create a new [`Signature`] instance.
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
            Eq | NotEq | Lt | LtEq | Gt | GtEq => match Self::comparision_coercion(lhs, rhs) {
                Some(data_type) => Ok(Self {
                    lhs: data_type.clone(),
                    rhs: data_type,
                    ret: Boolean,
                }),
                None => Err(coercion_err(lhs, op, rhs)),
            },
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
                let result = match op {
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

    /// Gets the result data type of an operation given the input data types and operator.
    pub fn get_result_type(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<DataType> {
        Self::try_new(lhs, op, rhs).map(|sig| sig.ret)
    }

    /// Gets the input data type of an operation given the input data types and operator.
    pub fn get_input_types(
        lhs: &DataType,
        op: &Operator,
        rhs: &DataType,
    ) -> Result<(DataType, DataType)> {
        Self::try_new(lhs, op, rhs).map(|sig| (sig.lhs, sig.rhs))
    }

    fn comparision_coercion(lhs: &DataType, rhs: &DataType) -> Option<DataType> {
        if lhs == rhs {
            return Some(lhs.clone());
        }

        Self::binary_numeric_coercion(lhs, rhs)
    }

    fn binary_numeric_coercion(lhs: &DataType, rhs: &DataType) -> Option<DataType> {
        use DataType::*;

        if !lhs.is_numeric() | !rhs.is_numeric() {
            return None;
        }

        match (lhs, rhs) {
            (Float64, _) | (_, Float64) => Some(Float64),
            (Float32, _) | (_, Float32) => Some(Float32),
            (Int64, _)
            | (_, Int64)
            | (UInt64, Int8)
            | (Int8, UInt64)
            | (UInt64, Int16)
            | (Int16, UInt64)
            | (UInt64, Int32)
            | (Int32, UInt64)
            | (UInt32, Int8)
            | (Int8, UInt32)
            | (UInt32, Int16)
            | (Int16, UInt32)
            | (UInt32, Int32)
            | (Int32, UInt32) => Some(Int64),
            (UInt64, _) | (_, UInt64) => Some(UInt64),
            (Int32, _)
            | (_, Int32)
            | (UInt16, Int16)
            | (Int16, UInt16)
            | (UInt16, Int8)
            | (Int8, UInt16) => Some(Int32),
            (UInt32, _) | (_, UInt32) => Some(UInt32),
            (Int16, _) | (_, Int16) | (Int8, UInt8) | (UInt8, Int8) => Some(Int16),
            (UInt16, _) | (_, UInt16) => Some(UInt16),
            (Int8, _) | (_, Int8) => Some(Int8),
            (UInt8, _) | (_, UInt8) => Some(UInt8),
            _ => None,
        }
    }
}

/// Coerces a binary expression and returns new coerced expression.
pub fn coerce_binary_expression(schema: &Schema, expression: &Expression) -> Result<Expression> {
    let coerced = match expression {
        Expression::Binary(e) => {
            let (lhs_type, rhs_type) = Signature::get_input_types(
                &e.lhs().data_type(schema)?,
                e.op(),
                &e.rhs().data_type(schema)?,
            )?;
            let lhs = Arc::new(e.lhs().cast_to(&lhs_type, schema)?);
            let rhs = Arc::new(e.rhs().cast_to(&rhs_type, schema)?);

            Expression::Binary(Binary::new(lhs, e.op().clone(), rhs))
        }
        _ => expression.clone(),
    };

    Ok(coerced)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::expression::operator::Operator;

    use super::Signature;

    #[test]
    fn test_signature_coercion() {
        let lhs = DataType::Int32;
        let rhs = DataType::Int64;
        let op = Operator::Eq;
        let sig = Signature::try_new(&lhs, &op, &rhs);
        let input_type = Signature::get_input_types(&lhs, &op, &rhs).unwrap();
        let result_type = Signature::get_result_type(&lhs, &op, &rhs).unwrap();

        assert!(sig.is_ok());
        assert_eq!(input_type, (DataType::Int64, DataType::Int64));
        assert_eq!(result_type, DataType::Boolean);
    }

    #[test]
    fn test_signature_numeric() {
        let lhs = DataType::Int64;
        let rhs = DataType::Int64;
        let op = Operator::Plus;
        let sig = Signature::try_new(&lhs, &op, &rhs);
        let input_type = Signature::get_input_types(&lhs, &op, &rhs).unwrap();
        let result_type = Signature::get_result_type(&lhs, &op, &rhs).unwrap();

        assert!(sig.is_ok());
        assert_eq!(input_type, (DataType::Int64, DataType::Int64));
        assert_eq!(result_type, DataType::Int64);
    }

    #[test]
    fn test_signature_bool() {
        let lhs = DataType::Boolean;
        let rhs = DataType::Boolean;
        let op = Operator::And;
        let sig = Signature::try_new(&lhs, &op, &rhs);
        let input_type = Signature::get_input_types(&lhs, &op, &rhs).unwrap();
        let result_type = Signature::get_result_type(&lhs, &op, &rhs).unwrap();

        assert!(sig.is_ok());
        assert_eq!(input_type, (DataType::Boolean, DataType::Boolean));
        assert_eq!(result_type, DataType::Boolean);
    }

    #[test]
    fn test_signature_cmp_coercion() {
        let lhs = DataType::Int64;
        let rhs = DataType::Int32;
        let op = Operator::Eq;
        let sig = Signature::try_new(&lhs, &op, &rhs);
        let input_type = Signature::get_input_types(&lhs, &op, &rhs).unwrap();
        let result_type = Signature::get_result_type(&lhs, &op, &rhs).unwrap();

        assert!(sig.is_ok());
        assert_eq!(input_type, (DataType::Int64, DataType::Int64));
        assert_eq!(result_type, DataType::Boolean);
    }

    #[test]
    fn test_signature_cmp() {
        let lhs = DataType::Int32;
        let rhs = DataType::Int32;
        let op = Operator::Eq;
        let sig = Signature::try_new(&lhs, &op, &rhs);
        let input_type = Signature::get_input_types(&lhs, &op, &rhs).unwrap();
        let result_type = Signature::get_result_type(&lhs, &op, &rhs).unwrap();

        assert!(sig.is_ok());
        assert_eq!(input_type, (DataType::Int32, DataType::Int32));
        assert_eq!(result_type, DataType::Boolean);
    }
}

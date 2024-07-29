use arrow::{
    array::{new_empty_array, ArrayRef},
    compute::kernels::numeric::{add_wrapping, div, mul_wrapping, sub_wrapping},
    datatypes::DataType,
};
use snafu::location;

use crate::error::{Error, Result};

use super::operator::Operator;

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

        // no type coercion supported yet
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
                if lhs != rhs {
                    return Err(coercion_err(lhs, op, rhs));
                }
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
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::expression::operator::Operator;

    use super::Signature;

    #[test]
    fn test_signature_coercion_err() {
        let lhs = DataType::Int32;
        let rhs = DataType::Int64;
        let op = Operator::Eq;
        let sig = Signature::try_new(&lhs, &op, &rhs);
        let input_type = Signature::get_input_types(&lhs, &op, &rhs);
        let result_type = Signature::get_result_type(&lhs, &op, &rhs);

        assert!(sig.is_err());
        assert!(input_type.is_err());
        assert!(result_type.is_err());
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

use std::{fmt::Display, hash::Hash, iter, sync::Arc};

use crate::error::{Error, Result};
use arrow::{
    array::{
        make_array, Array, ArrayData, ArrayRef, BooleanArray, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveArray, Scalar, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{ArrowPrimitiveType, DataType},
};
use snafu::location;

/// Representing the return value of a physical expression.
/// Which is either an arrow `Array` or a `Scalar` value.
#[derive(Debug)]
pub enum ColumnarValue {
    /// An arrow array.
    Array(ArrayRef),
    /// A `ScalarValue`.
    Scalar(ScalarValue),
}

impl ColumnarValue {
    /// Convert the variant into an [`arrow::array::ArrayRef`].
    pub fn into_array(self, num_rows: usize) -> Result<ArrayRef> {
        use ColumnarValue::*;

        Ok(match self {
            Array(e) => e,
            Scalar(e) => e.to_array(num_rows),
        })
    }
}

/// Macro to build an array from an optional scalar value.
macro_rules! build_array_from_option {
    ($data_type:ident, $array_type:ident, $expr:expr, $size:expr) => {
        match $expr {
            Some(v) => Arc::new($array_type::from_value(*v, $size)),
            None => make_array(ArrayData::new_null(&DataType::$data_type, $size)),
        }
    };
}

/// Macro to cast an array element to a specific scalar type.
macro_rules! typed_cast {
    ($arr:expr, $idx:expr, $ty:ident, $scalar:ident) => {{
        let array =
            $arr.as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| crate::error::Error::Arrow {
                    message: "Failed to downcast array".to_string(),
                    location: snafu::location!(),
                })?;
        let value = match array.is_null($idx) {
            true => None,
            false => Some(array.value($idx).into()),
        };
        Ok::<ScalarValue, crate::error::Error>(ScalarValue::$scalar(value))
    }};
}

/// An enum representing the different types of `ScalarValue`'s.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<String>),
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;

        match (self, other) {
            (Null, Null) => true,
            (Null, _) => false,
            (Boolean(l), Boolean(r)) => l.eq(r),
            (Boolean(_), _) => false,
            (Int8(l), Int8(r)) => l.eq(r),
            (Int8(_), _) => false,
            (Int16(l), Int16(r)) => l.eq(r),
            (Int16(_), _) => false,
            (Int32(l), Int32(r)) => l.eq(r),
            (Int32(_), _) => false,
            (Int64(l), Int64(r)) => l.eq(r),
            (Int64(_), _) => false,
            (UInt8(l), UInt8(r)) => l.eq(r),
            (UInt8(_), _) => false,
            (UInt16(l), UInt16(r)) => l.eq(r),
            (UInt16(_), _) => false,
            (UInt32(l), UInt32(r)) => l.eq(r),
            (UInt32(_), _) => false,
            (UInt64(l), UInt64(r)) => l.eq(r),
            (UInt64(_), _) => false,
            (Utf8(l), Utf8(r)) => l.eq(r),
            (Utf8(_), _) => false,
            (Float32(l), Float32(r)) => match (l, r) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => l.eq(r),
            },
            (Float32(_), _) => false,
            (Float64(l), Float64(r)) => match (l, r) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => l.eq(r),
            },
            (Float64(_), _) => false,
        }
    }
}

impl Eq for ScalarValue {}

struct FloatWrapper<T>(T);

impl Hash for FloatWrapper<f32> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(&f32::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
    }
}

impl Hash for FloatWrapper<f64> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(&f64::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
    }
}

impl Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use ScalarValue::*;

        match self {
            Null => 1.hash(state),
            Boolean(v) => v.hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8(v) => v.hash(state),
            Float32(v) => v.map(FloatWrapper).hash(state),
            Float64(v) => v.map(FloatWrapper).hash(state),
        }
    }
}

impl ScalarValue {
    /// Create a [`ScalarValue`] from provided value and datatype.
    pub fn new_primitive<T: ArrowPrimitiveType>(
        value: Option<T::Native>,
        data_type: &DataType,
    ) -> Result<Self> {
        match value {
            None => data_type.try_into(),
            Some(v) => {
                let array = PrimitiveArray::<T>::new(vec![v].into(), None)
                    .with_data_type(data_type.clone());
                Self::try_from_array(&array, 0)
            }
        }
    }

    /// Attempts to create a [`ScalarValue`] from an array element.
    pub fn try_from_array(array: &dyn Array, index: usize) -> Result<Self> {
        if !array.is_valid(index) {
            return array.data_type().try_into();
        }

        Ok(match array.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean)?,
            DataType::Int8 => typed_cast!(array, index, Int8Array, Int8)?,
            DataType::Int16 => typed_cast!(array, index, Int16Array, Int16)?,
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32)?,
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64)?,
            DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8)?,
            DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16)?,
            DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32)?,
            DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64)?,
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8)?,
            DataType::Float32 => typed_cast!(array, index, Float32Array, Float32)?,
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64)?,
            other => {
                return Err(Error::InvalidOperation {
                    message: format!(
                        "Creating a ScalarValue from array with datatype '{}' is not supported",
                        other
                    ),
                    location: location!(),
                });
            }
        })
    }

    /// Retrieves the data type of the [`ScalarValue`].
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
        }
    }

    /// Whether this value is null or not
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Null => true,
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt8(v) => v.is_none(),
            ScalarValue::UInt16(v) => v.is_none(),
            ScalarValue::UInt32(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
            ScalarValue::Utf8(v) => v.is_none(),
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
        }
    }

    /// Converts the [`ScalarValue`] into a `Scalar<ArrayRef>`.
    pub fn to_scalar(&self) -> Result<Scalar<ArrayRef>> {
        Ok(Scalar::new(self.to_array(1)))
    }

    /// Converts the [`ScalarValue`] into an `ArrayRef` with the specified number of rows.
    pub fn to_array(&self, num_rows: usize) -> ArrayRef {
        match self {
            ScalarValue::Null => make_array(ArrayData::new_null(&DataType::Null, num_rows)),
            ScalarValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; num_rows])) as ArrayRef,
            ScalarValue::Int8(v) => build_array_from_option!(Int8, Int8Array, v, num_rows),
            ScalarValue::Int16(v) => build_array_from_option!(Int16, Int16Array, v, num_rows),
            ScalarValue::Int32(v) => build_array_from_option!(Int32, Int32Array, v, num_rows),
            ScalarValue::Int64(v) => build_array_from_option!(Int64, Int64Array, v, num_rows),
            ScalarValue::UInt8(v) => build_array_from_option!(UInt8, UInt8Array, v, num_rows),
            ScalarValue::UInt16(v) => build_array_from_option!(UInt16, UInt16Array, v, num_rows),
            ScalarValue::UInt32(v) => build_array_from_option!(UInt32, UInt32Array, v, num_rows),
            ScalarValue::UInt64(v) => build_array_from_option!(UInt64, UInt64Array, v, num_rows),
            ScalarValue::Float32(v) => build_array_from_option!(Float32, Float32Array, v, num_rows),
            ScalarValue::Float64(v) => build_array_from_option!(Float64, Float64Array, v, num_rows),
            ScalarValue::Utf8(v) => match v {
                Some(v) => Arc::new(StringArray::from_iter_values(
                    iter::repeat(v).take(num_rows),
                )),
                None => make_array(ArrayData::new_null(&DataType::Utf8, num_rows)),
            },
        }
    }
}

impl TryFrom<DataType> for ScalarValue {
    type Error = Error;

    /// Tries to create a `ScalarValue` from a `DataType`.
    fn try_from(data_type: DataType) -> Result<Self> {
        (&data_type).try_into()
    }
}

impl TryFrom<&DataType> for ScalarValue {
    type Error = Error;

    /// Tries to create a `ScalarValue` from a reference to a `DataType`.
    fn try_from(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => ScalarValue::Boolean(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::UInt8 => ScalarValue::UInt8(None),
            DataType::UInt16 => ScalarValue::UInt16(None),
            DataType::UInt32 => ScalarValue::UInt32(None),
            DataType::UInt64 => ScalarValue::UInt64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Utf8 => ScalarValue::Utf8(None),
            _ => {
                return Err(Error::InvalidOperation {
                    message: format!(
                        "TryFrom DataType '{}' to ScalarValue is not supported",
                        data_type
                    ),
                    location: location!(),
                })
            }
        })
    }
}

macro_rules! format_option {
    ($f:expr, $expr:expr) => {
        match $expr {
            Some(v) => write!($f, "{}", v),
            None => write!($f, "NULL"),
        }
    };
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Boolean(v) => format_option!(f, v),
            ScalarValue::Int8(v) => format_option!(f, v),
            ScalarValue::Int16(v) => format_option!(f, v),
            ScalarValue::Int32(v) => format_option!(f, v),
            ScalarValue::Int64(v) => format_option!(f, v),
            ScalarValue::UInt8(v) => format_option!(f, v),
            ScalarValue::UInt16(v) => format_option!(f, v),
            ScalarValue::UInt32(v) => format_option!(f, v),
            ScalarValue::UInt64(v) => format_option!(f, v),
            ScalarValue::Float32(v) => format_option!(f, v),
            ScalarValue::Float64(v) => format_option!(f, v),
            ScalarValue::Utf8(v) => format_option!(f, v),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::DataType,
    };

    use super::ScalarValue;

    #[test]
    fn test_scalar_value() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let scalar_value = ScalarValue::try_from_array(&arr, 0).unwrap();
        let scalar = scalar_value.to_scalar().unwrap();
        let scalar_arr = scalar_value.to_array(2);

        assert_eq!(scalar_value.data_type(), DataType::Int32);
        assert_eq!(scalar.into_inner().data_type(), &DataType::Int32);
        assert_eq!(scalar_arr.len(), 2);
        assert_eq!(scalar_arr.data_type(), &DataType::Int32);
    }
}

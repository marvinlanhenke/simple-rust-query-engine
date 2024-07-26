use crate::error::Result;
use arrow::array::ArrayRef;

#[derive(Debug)]
pub enum ColumnarValue {
    Array(ArrayRef),
    Scalar(ScalarValue),
}

impl ColumnarValue {
    pub fn into_array(self, num_rows: usize) -> Result<ArrayRef> {
        use ColumnarValue::*;

        Ok(match self {
            Array(v) => v,
            Scalar(v) => v.to_array(num_rows),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    Utf8(Option<String>),
}

impl ScalarValue {
    pub fn to_array(&self, _num_rows: usize) -> ArrayRef {
        todo!()
    }
}

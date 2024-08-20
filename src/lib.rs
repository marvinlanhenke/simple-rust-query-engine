//! # Simple Rust Query Engine

pub mod error;
pub mod execution;
pub mod expression;
pub mod io;
pub mod optimize;
pub mod plan;
pub mod utils;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    /// A helper function to generate a schema.
    /// Corresponds to the `testdata/csv/simple.csv` file.
    pub fn create_schema() -> Schema {
        Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
        ])
    }

    /// A helper function to generate a sample [`RecordBatch`].
    pub fn create_record_batch() -> RecordBatch {
        let schema = Arc::new(create_schema());
        let utf_arr = Arc::new(StringArray::from(vec!["hello", "world"]));
        let int_arr1 = Arc::new(Int64Array::from(vec![1, 2]));
        let int_arr2 = Arc::new(Int64Array::from(vec![11, 22]));

        RecordBatch::try_new(schema, vec![utf_arr, int_arr1, int_arr2]).unwrap()
    }

    /// A helper function to generate a sample [`RecordBatch`].
    pub fn create_record_batch_with_nulls() -> RecordBatch {
        let schema = Arc::new(create_schema());
        let utf_arr = Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")]));
        let int_arr1 = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let int_arr2 = Arc::new(Int64Array::from(vec![None, Some(11), Some(22)]));

        RecordBatch::try_new(schema, vec![utf_arr, int_arr1, int_arr2]).unwrap()
    }
}

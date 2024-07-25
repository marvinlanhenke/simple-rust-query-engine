//! # Simple Rust Query Engine

pub mod context;
pub mod dataframe;
pub mod error;
pub mod expression;
pub mod io;
pub mod plan;

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    /// A helper function to generate a schema.
    /// Corresponds to the `testdata/csv/simple.csv` file.
    pub fn create_schema() -> Schema {
        Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
        ])
    }
}

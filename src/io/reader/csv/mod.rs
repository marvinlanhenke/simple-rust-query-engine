pub mod opener;
pub mod options;
pub mod source;

/// How many records should be read in order to infer the CSV schema.
pub const MAX_INFER_RECORDS: usize = 100;

/// The default number of CSV records to read per batch.
pub const DEFAULT_BATCH_SIZE: usize = 1024;

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

use std::{fs::File, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::error::Result;

/// How many records should be read in order to infer the CSV schema.
const MAX_INFER_RECORDS: usize = 100;

/// A builder for [`CsvReadOptions`].
#[derive(Debug)]
pub struct CsvReadOptionsBuilder {
    /// An optional [`arrow::datatypes::Schema`].
    schema: Option<SchemaRef>,
    /// Whether the first row should be treated as a header.
    has_header: bool,
    /// The character used as a field delimiter.
    delimiter: u8,
    /// The character used for quoting fields.
    quote: u8,
}

impl CsvReadOptionsBuilder {
    /// Creates a [`CsvReadOptionsBuilder`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a reference-counted [`arrow::datatypes::Schema`].
    pub fn with_schema(mut self, schema: Option<SchemaRef>) -> Self {
        self.schema = schema;
        self
    }

    /// Adds a boolean flag, whether a header is present or not.
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Adds a byte for the `delimiter`.
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Adds a byte for the `quote`.
    pub fn quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// Builds the final [`CsvReadOptions`].
    pub fn build(self) -> CsvReadOptions {
        CsvReadOptions {
            schema: self.schema,
            has_header: self.has_header,
            delimiter: self.delimiter,
            quote: self.quote,
        }
    }
}

impl Default for CsvReadOptionsBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            has_header: true,
            delimiter: b',',
            quote: b'"',
        }
    }
}

/// Configuration options for reading CSV files.
#[derive(Debug)]
pub struct CsvReadOptions {
    /// An optional [`arrow::datatypes::SchemaRef`].
    schema: Option<SchemaRef>,
    /// Whether the first row should be treated as a header.
    has_header: bool,
    /// The character used as a field delimiter.
    delimiter: u8,
    /// The character used for quoting fields.
    quote: u8,
}

impl CsvReadOptions {
    /// Creates a [`CsvReadOptions`] instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a [`CsvReadOptionsBuilder`].
    pub fn builder() -> CsvReadOptionsBuilder {
        CsvReadOptionsBuilder::default()
    }
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        CsvReadOptionsBuilder::new().build()
    }
}

/// Responsible for providing access to a CSV data source.
#[derive(Debug)]
pub struct CsvDataSource {
    /// The filesystem path to the CSV file.
    path: String,
    /// A reference-counted [`arrow::datatypes::Schema`].
    schema: SchemaRef,
    /// Configuration options for reading a CSV file.
    options: CsvReadOptions,
}

impl CsvDataSource {
    /// Attempts to create a new [`CsvDataSource`].
    ///
    /// If no [`arrow::datatypes::Schema`] is provided via the [`CsvReadOptions`]
    /// the schema will be infered, if possible.
    pub fn try_new(path: impl Into<String>, options: CsvReadOptions) -> Result<Self> {
        let path = path.into();
        let schema = match &options.schema {
            Some(schema) => schema.clone(),
            None => Self::infer_schema(&path, &options)?,
        };

        Ok(Self {
            path,
            schema,
            options,
        })
    }

    /// The filepath of the CSV file.
    pub fn path(&self) -> &str {
        self.path.as_ref()
    }

    /// A reference-counted [`arrow::datatypes::Schema`].
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// A [`CsvReadOptions`] reference.
    pub fn options(&self) -> &CsvReadOptions {
        &self.options
    }

    /// Infers the schema for a CSV file.
    ///
    /// This function opens the CSV file located at the given `path` and uses
    /// the Arrow CSV reader to read the file and infer its schema. The schema
    /// inference process reads up to `max_records` to determine the data types
    /// of the columns.
    fn infer_schema(path: &str, options: &CsvReadOptions) -> Result<SchemaRef> {
        let file = File::open(path)?;
        let format = arrow::csv::reader::Format::default()
            .with_header(options.has_header)
            .with_delimiter(options.delimiter)
            .with_quote(options.quote);
        let (schema, _) = format.infer_schema(file, Some(MAX_INFER_RECORDS))?;

        Ok(Arc::new(schema))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{CsvDataSource, CsvReadOptions};

    fn create_schema() -> Schema {
        Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
        ])
    }

    #[test]
    fn test_csv_datasource_infer_schema() {
        let options = CsvReadOptions::new();
        let path = "testdata/csv/simple.csv";
        let source = CsvDataSource::try_new(path, options).unwrap();

        let result = source.schema();
        let expected = Arc::new(create_schema());

        assert_eq!(result, expected);
    }
}

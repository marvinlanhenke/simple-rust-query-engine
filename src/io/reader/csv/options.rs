use arrow::datatypes::SchemaRef;

use super::DEFAULT_BATCH_SIZE;

/// A builder for [`CsvReadOptions`].
#[derive(Debug)]
pub struct CsvReadOptionsBuilder {
    /// An optional [`arrow::datatypes::Schema`].
    schema: Option<SchemaRef>,
    /// Whether the first row should be treated as a header.
    has_header: bool,
    /// The byte used as a field delimiter.
    delimiter: u8,
    /// The byte used for quoting fields.
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
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Adds a byte for the `quote`.
    pub fn with_quote(mut self, quote: u8) -> Self {
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

    /// An optional [`arrow::datatypes::SchemaRef`].
    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }

    /// Whether the first row should be treated as a header.
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// The character used as a field delimiter.
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// The character used for quoting fields.
    pub fn quote(&self) -> u8 {
        self.quote
    }
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        CsvReadOptionsBuilder::new().build()
    }
}

pub struct CsvFileOpenerConfigBuilder {
    /// The number of records to read per batch.
    batch_size: usize,
    /// A reference-counted [`arrow::datatypes::Schema`].
    schema: SchemaRef,
    /// An optional list of column indices to project.
    projection: Option<Vec<usize>>,
    /// Whether the first row should be treated as a header.
    has_header: bool,
    /// The byte used as a field delimiter.
    delimiter: u8,
    /// The byte used for quoting fields.
    quote: u8,
}

impl CsvFileOpenerConfigBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            schema,
            projection: None,
            has_header: true,
            delimiter: b',',
            quote: b'"',
        }
    }
    /// Adds a batch size, defining the number of records to read per batch.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Adds a reference-counted [`arrow::datatypes::Schema`].
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    /// Adds a projection of column indices.
    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    /// Adds a boolean flag, whether a header is present or not.
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Adds a byte for the `delimiter`.
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Adds a byte for the `quote`.
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// Builds the final [`CsvFileOpenerConfig`].
    pub fn build(self) -> CsvFileOpenerConfig {
        CsvFileOpenerConfig {
            batch_size: self.batch_size,
            schema: self.schema,
            projection: self.projection,
            has_header: self.has_header,
            delimiter: self.delimiter,
            quote: self.quote,
        }
    }
}

/// Configuration parameters for opening,
/// reading, and streaming the contents of a CSV file.
#[derive(Debug, Clone)]
pub struct CsvFileOpenerConfig {
    /// The number of records to read per batch.
    batch_size: usize,
    /// A reference-counted [`arrow::datatypes::Schema`].
    schema: SchemaRef,
    /// An optional list of column indices to project.
    projection: Option<Vec<usize>>,
    /// Whether the first row should be treated as a header.
    has_header: bool,
    /// The character used as a field delimiter.
    delimiter: u8,
    /// The character used for quoting fields.
    quote: u8,
}

impl CsvFileOpenerConfig {
    /// Creates a [`CsvFileOpenerConfig`] instance.
    pub fn new(schema: SchemaRef) -> Self {
        CsvFileOpenerConfigBuilder::new(schema).build()
    }

    /// Creates a [`CsvFileOpenerConfigBuilder`].
    pub fn builder(schema: SchemaRef) -> CsvFileOpenerConfigBuilder {
        CsvFileOpenerConfigBuilder::new(schema)
    }

    /// The number of record to read per batch.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// A reference-counted [`arrow::datatypes::Schema`].
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// An optional list of column indices to project.
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// Whether the first row should be treated as a header.
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// The byte used as a field delimiter.
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// The byte used for quoting fields.
    pub fn quote(&self) -> u8 {
        self.quote
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::tests::create_schema;

    use super::{CsvFileOpenerConfig, CsvReadOptions};

    #[test]
    fn test_csv_file_config_builder() {
        let schema = Arc::new(create_schema());
        let projection = Some(vec![1, 2, 3]);
        let config = CsvFileOpenerConfig::builder(schema)
            .with_batch_size(2048)
            .with_header(false)
            .with_projection(projection.clone())
            .build();

        assert!(!config.has_header());
        assert_eq!(config.batch_size(), 2048);
        assert_eq!(config.projection(), projection.as_ref());
    }

    #[test]
    fn test_csv_options_builder() {
        let schema = Arc::new(create_schema());
        let options = CsvReadOptions::builder()
            .with_header(false)
            .with_delimiter(b',')
            .with_quote(b'"')
            .with_schema(Some(schema.clone()))
            .build();

        assert!(!options.has_header);
        assert_eq!(options.delimiter, b',');
        assert_eq!(options.quote, b'"');
        assert_eq!(options.schema, Some(schema));
    }
}

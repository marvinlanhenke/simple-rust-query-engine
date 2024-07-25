use std::{fs::File, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{error::Result, io::DataSource};

use super::{options::CsvReadOptions, MAX_INFER_RECORDS};

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
        let schema = match &options.schema() {
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
            .with_header(options.has_header())
            .with_delimiter(options.delimiter())
            .with_quote(options.quote());
        let (schema, _) = format.infer_schema(file, Some(MAX_INFER_RECORDS))?;

        Ok(Arc::new(schema))
    }
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        io::{reader::csv::options::CsvReadOptions, DataSource},
        tests::create_schema,
    };

    use super::CsvDataSource;

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

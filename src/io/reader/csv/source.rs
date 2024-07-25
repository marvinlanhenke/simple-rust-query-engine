use std::{fs::File, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{
    error::Result,
    io::{reader::csv::options::CsvFileOpenerConfig, DataSource},
    plan::physical::{plan::ExecutionPlan, scan::csv::CsvExec},
};

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

    fn scan(&self, projection: Option<&Vec<String>>) -> Result<Arc<dyn ExecutionPlan>> {
        let projection_idx = projection.map(|proj| {
            proj.iter()
                .filter_map(|name| self.schema.column_with_name(name).map(|(idx, _)| idx))
                .collect::<Vec<_>>()
        });
        let config = CsvFileOpenerConfig::builder(self.schema.clone())
            .with_projection(projection_idx)
            .build();
        let exec = CsvExec::new(&self.path, config);

        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        io::{reader::csv::options::CsvReadOptions, DataSource},
        plan::physical::scan::csv::CsvExec,
        tests::create_schema,
    };

    use super::CsvDataSource;

    #[test]
    fn test_csv_datasource_scan_with_projection() {
        let options = CsvReadOptions::new();
        let path = "testdata/csv/simple.csv";
        let source = CsvDataSource::try_new(path, options).unwrap();
        let projection = Some(vec!["c1".to_string()]);

        let exec = source.scan(projection.as_ref()).unwrap();
        let exec = exec.as_any().downcast_ref::<CsvExec>().unwrap();
        assert!(exec.config().projection().is_some());
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

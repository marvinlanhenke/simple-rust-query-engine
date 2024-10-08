use std::sync::Arc;

use arrow_schema::SchemaRef;

use crate::{
    error::Result,
    io::{reader::csv::options::CsvFileOpenerConfig, DataSource, FileFormat},
    plan::physical::{plan::ExecutionPlan, scan::csv::CsvExec},
};

/// Represents a table that is loaded from a file, such as a CSV file.
/// It holds the name, file path, schema, and file format information of the table.
#[derive(Debug, Clone)]
pub struct ListingTable {
    name: String,
    path: String,
    schema: SchemaRef,
    file_format: FileFormat,
}

impl ListingTable {
    /// Creates a new `ListingTable` instance.
    pub fn new(
        name: impl Into<String>,
        path: impl Into<String>,
        schema: SchemaRef,
        file_format: FileFormat,
    ) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            schema,
            file_format,
        }
    }

    /// Returns the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the file path of the table.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the file format of the table.
    pub fn file_format(&self) -> FileFormat {
        self.file_format
    }
}

impl DataSource for ListingTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<&Vec<String>>) -> Result<Arc<dyn ExecutionPlan>> {
        match self.file_format {
            FileFormat::Csv => {
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
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::{
        io::{reader::listing::table::ListingTable, DataSource, FileFormat},
        tests::create_schema,
    };

    #[tokio::test]
    async fn test_listing_table_scan_with_projection() {
        let schema = create_schema();
        let table = ListingTable::new(
            "simple",
            "testdata/csv/simple.csv",
            Arc::new(schema),
            FileFormat::Csv,
        );
        let exec = table.scan(Some(&vec!["c1".to_string()])).unwrap();

        let mut stream = exec.execute().unwrap();

        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 6);
            assert_eq!(batch.num_columns(), 1);
        }
    }

    #[tokio::test]
    async fn test_listing_table_scan_no_projection() {
        let schema = create_schema();
        let table = ListingTable::new(
            "simple",
            "testdata/csv/simple.csv",
            Arc::new(schema),
            FileFormat::Csv,
        );
        let exec = table.scan(None).unwrap();

        let mut stream = exec.execute().unwrap();

        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 6);
            assert_eq!(batch.num_columns(), 3);
        }
    }
}

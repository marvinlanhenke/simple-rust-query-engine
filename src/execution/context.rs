use std::sync::Arc;

use crate::{
    error::Result,
    io::reader::{
        csv::{options::CsvReadOptions, source::CsvDataSource},
        listing::table::ListingTable,
    },
    plan::logical::{plan::LogicalPlan, scan::Scan},
};

use super::dataframe::DataFrame;

/// Represents a [`SessionContext`] for managing query execution.
#[derive(Debug, Default)]
pub struct SessionContext {
    table: Option<ListingTable>,
}

impl SessionContext {
    /// Creates a new [`SessionContext`] instance.
    pub fn new() -> Self {
        Self { table: None }
    }

    /// Reads a CSV file and creates a `DataFrame`.
    pub fn read_csv(&self, path: impl Into<String>, options: CsvReadOptions) -> Result<DataFrame> {
        let path = path.into();
        let source = CsvDataSource::try_new(&path, options)?;
        let plan = LogicalPlan::Scan(Scan::new(&path, Arc::new(source), None, vec![]));

        Ok(DataFrame::new(plan))
    }

    pub fn register_csv(&mut self, name: &str, path: &str, options: CsvReadOptions) -> Result<()> {
        let resolved_schema = match options.schema() {
            Some(schema) => schema,
            None => CsvDataSource::infer_schema(path, &options)?,
        };
        let source = Arc::new(CsvDataSource::try_new(path, options)?);
        let table = ListingTable::new(name, path, resolved_schema, source);
        self.table = Some(table);

        Ok(())
    }
}

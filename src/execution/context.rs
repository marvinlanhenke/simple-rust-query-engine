use std::sync::Arc;

use crate::{
    error::Result,
    io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
    plan::logical::{plan::LogicalPlan, scan::Scan},
};

use super::dataframe::DataFrame;

/// Represents a [`SessionContext`] for managing query execution.
#[derive(Debug, Default, Clone, Copy)]
pub struct SessionContext;

impl SessionContext {
    /// Creates a new [`SessionContext`] instance.
    pub fn new() -> Self {
        Self {}
    }

    /// Reads a CSV file and creates a `DataFrame`.
    pub fn read_csv(self, path: impl Into<String>, options: CsvReadOptions) -> Result<DataFrame> {
        let path = path.into();
        let source = CsvDataSource::try_new(&path, options)?;
        let plan = LogicalPlan::Scan(Scan::new(&path, Arc::new(source), None, vec![]));

        Ok(DataFrame::new(plan))
    }
}

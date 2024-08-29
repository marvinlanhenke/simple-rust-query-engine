use std::sync::Arc;

use crate::{
    error::Result,
    io::{
        reader::{
            csv::{options::CsvReadOptions, source::CsvDataSource},
            listing::table::ListingTable,
        },
        FileFormat,
    },
    plan::logical::{plan::LogicalPlan, scan::Scan},
    sql::parser::WrappedParser,
};

use super::dataframe::DataFrame;

/// Represents a [`SessionContext`] for managing query execution.
#[derive(Debug, Default, Clone)]
pub struct SessionContext {
    table: Option<ListingTable>,
}

impl SessionContext {
    /// Creates a new [`SessionContext`] instance.
    pub fn new() -> Self {
        Self { table: None }
    }

    /// Retrieves the current registered `ListingTable`.
    pub fn table(&self) -> Option<&ListingTable> {
        self.table.as_ref()
    }

    /// Reads a CSV file and creates a `DataFrame`.
    pub fn read_csv(&self, path: impl Into<String>, options: CsvReadOptions) -> Result<DataFrame> {
        let path = path.into();
        let source = CsvDataSource::try_new(&path, options)?;
        let plan = LogicalPlan::Scan(Scan::new(&path, Arc::new(source), None, vec![]));

        Ok(DataFrame::new(plan))
    }

    /// Registers a CSV `ListingTable` with the current `SessionContext`.
    pub fn register_csv(&self, name: &str, path: &str, options: CsvReadOptions) -> Result<Self> {
        let resolved_schema = match options.schema() {
            Some(schema) => schema,
            None => CsvDataSource::infer_schema(path, &options)?,
        };
        let table = Some(ListingTable::new(
            name,
            path,
            resolved_schema,
            FileFormat::Csv,
        ));

        Ok(Self { table })
    }

    /// Creates a `DataFrame` from SQL query text.
    pub fn sql(&self, sql: &str) -> Result<DataFrame> {
        // create logical plan
        // 1. sql_to_statement
        // 2. statement_to_plan
        let mut parser = WrappedParser::try_new(sql)?;
        let _statement = parser.try_parse()?;

        // return dataframe with plan
        // Ok(DataFrame::new(plan))
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::io::{reader::csv::options::CsvReadOptions, DataSource};

    use super::SessionContext;

    #[test]
    fn test_context_register_csv() {
        let ctx = SessionContext::new();
        let ctx = ctx
            .register_csv("simple", "testdata/csv/simple.csv", CsvReadOptions::new())
            .unwrap();
        let table = ctx.table().unwrap();

        assert_eq!(table.name(), "simple");
        assert_eq!(table.schema().fields().len(), 3);
    }
}

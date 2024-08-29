use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockReadGuard},
};

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
#[derive(Debug, Default)]
pub struct SessionContext {
    tables: RwLock<HashMap<String, ListingTable>>,
}

impl SessionContext {
    /// Creates a new [`SessionContext`] instance.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Retrieves the current registered `ListingTable`.
    pub fn tables(&self) -> RwLockReadGuard<HashMap<String, ListingTable>> {
        self.tables.read().unwrap()
    }

    /// Reads a CSV file and creates a `DataFrame`.
    pub fn read_csv(&self, path: impl Into<String>, options: CsvReadOptions) -> Result<DataFrame> {
        let path = path.into();
        let source = CsvDataSource::try_new(&path, options)?;
        let plan = LogicalPlan::Scan(Scan::new(&path, Arc::new(source), None, vec![]));

        Ok(DataFrame::new(plan))
    }

    /// Registers a CSV `ListingTable` with the current `SessionContext`.
    pub fn register_csv(&self, name: &str, path: &str, options: CsvReadOptions) -> Result<()> {
        let resolved_schema = match options.schema() {
            Some(schema) => schema,
            None => CsvDataSource::infer_schema(path, &options)?,
        };
        let table = ListingTable::new(name, path, resolved_schema, FileFormat::Csv);

        self.tables.write().unwrap().insert(name.to_string(), table);

        Ok(())
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
        ctx.register_csv("simple", "testdata/csv/simple.csv", CsvReadOptions::new())
            .unwrap();
        let tables = ctx.tables();
        let table = tables.get("simple").unwrap();

        assert_eq!(table.name(), "simple");
        assert_eq!(table.schema().fields().len(), 3);
    }
}

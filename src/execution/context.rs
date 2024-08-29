use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use snafu::location;
use sqlparser::ast::Statement;

use crate::{
    error::{Error, Result},
    io::{
        reader::{
            csv::{options::CsvReadOptions, source::CsvDataSource},
            listing::table::ListingTable,
        },
        FileFormat,
    },
    plan::logical::{plan::LogicalPlan, scan::Scan},
    sql::{parser::WrappedParser, select::query_to_plan, visitor::resolve_table_references},
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
        let mut parser = WrappedParser::try_new(sql)?;
        let statement = parser.try_parse()?;

        let references = resolve_table_references(&statement)?;
        self.validate_references(&references)?;

        let plan = match statement {
            Statement::Query(query) => query_to_plan(*query, &self.tables())?,
            _ => {
                return Err(Error::InvalidOperation {
                    message: format!("SQL statement {} is not supported yet", statement),
                    location: location!(),
                })
            }
        };

        Ok(DataFrame::new(plan))
    }

    fn validate_references(&self, references: &[String]) -> Result<()> {
        let tables = self.tables();
        for reference in references {
            if !tables.contains_key(reference) {
                return Err(Error::InvalidData {
                    message: format!(
                        "Table reference {} is not registered with the SessionContext",
                        reference
                    ),
                    location: location!(),
                });
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::io::{reader::csv::options::CsvReadOptions, DataSource};

    use super::SessionContext;

    #[test]
    fn test_context_validate_references() {
        let ctx = SessionContext::new();
        ctx.register_csv("simple", "testdata/csv/simple.csv", CsvReadOptions::new())
            .unwrap();

        let res_ok = ctx.validate_references(&["simple".to_string()]);
        let res_err1 = ctx.validate_references(&["not_here".to_string()]);
        let res_err2 = ctx.validate_references(&["simple".to_string(), "not_here".to_string()]);

        assert!(res_ok.is_ok());
        assert!(res_err1.is_err());
        assert!(res_err2.is_err());
    }

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

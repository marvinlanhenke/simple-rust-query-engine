use std::fmt::Display;

use crate::{
    error::{Error, Result},
    plan::logical::plan::LogicalPlan,
};
use arrow::datatypes::{Field, SchemaRef};
use snafu::location;

/// Represents a [`Column`] expression in an AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    /// The name of the column.
    name: String,
}

impl Column {
    /// Creates a new [`Column`] instance.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// The name of the column.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Resolves this column to its [`Field`] definition from a logical plan.
    pub fn to_field_from_plan(&self, plan: &LogicalPlan) -> Result<Field> {
        self.to_field(plan.schema())
    }

    /// Resolves this column to its [`Field`] definition from a schema.
    pub fn to_field(&self, schema: SchemaRef) -> Result<Field> {
        let (_, field) = schema
            .column_with_name(&self.name)
            .ok_or_else(|| Error::InvalidData {
                message: format!(
                    "Column with name '{}' could not be found in schema",
                    &self.name
                ),
                location: location!(),
            })?;
        Ok(field.clone())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::{
        expression::logical::expr::Expression,
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        plan::logical::{plan::LogicalPlan, scan::Scan},
        tests::create_schema,
    };

    use super::Column;

    #[test]
    fn test_column_to_field() {
        let schema = Arc::new(create_schema());
        let options = CsvReadOptions::builder().with_schema(Some(schema)).build();
        let source = CsvDataSource::try_new("test_path", options).unwrap();
        let plan = LogicalPlan::Scan(Scan::new("test_path", Arc::new(source), None, vec![]));

        let cols = [
            ("c1", DataType::Utf8, Expression::Column(Column::new("c1"))),
            ("c2", DataType::Int64, Expression::Column(Column::new("c2"))),
            ("c3", DataType::Int64, Expression::Column(Column::new("c3"))),
        ];

        for (name, data_type, col) in cols.iter() {
            let field = col.to_field(&plan).unwrap();
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), data_type);
        }
    }
}

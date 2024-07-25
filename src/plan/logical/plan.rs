use std::fmt::Display;

use arrow::datatypes::SchemaRef;

use crate::expression::logical::expr::Expression;

use super::scan::Scan;

/// Represents a [`LogicalPlan`] for query execution.
#[derive(Debug)]
pub enum LogicalPlan {
    /// A [`Scan`] operation on the [`DataSource`].
    Scan(Scan),
}

impl LogicalPlan {
    /// A reference-counted [`arrow::datatypes::Schema`].
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan(plan) => plan.schema(),
        }
    }

    /// Retrieves the child logical plans.
    pub fn children(&self) -> &[&LogicalPlan] {
        match self {
            LogicalPlan::Scan(plan) => plan.children(),
        }
    }

    /// Retrieves the expressions associated with the logical plan.
    pub fn expressions(&self) -> &[&Expression] {
        match self {
            LogicalPlan::Scan(plan) => plan.expressions(),
        }
    }

    /// Formats the logical plan for display purposes with indentation.
    fn format(&self, f: &mut std::fmt::Formatter<'_>, indent: usize) -> std::fmt::Result {
        for _ in 0..indent {
            write!(f, "\t")?;
        }

        match self {
            LogicalPlan::Scan(plan) => write!(f, "{}", plan)?,
        }
        writeln!(f)?;

        for child in self.children() {
            child.format(f, indent + 1)?;
        }

        Ok(())
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f, 0)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::{
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        plan::logical::scan::Scan,
    };

    use super::LogicalPlan;

    fn create_csv_source() -> CsvDataSource {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int16, true)]));
        let options = CsvReadOptions::builder().with_schema(Some(schema)).build();
        CsvDataSource::try_new("test_path", options).unwrap()
    }

    #[test]
    fn test_logical_plan_scan() {
        let source = Arc::new(create_csv_source());
        let projection = Some(vec!["a".to_string()]);
        let scan = LogicalPlan::Scan(Scan::new("test_path", source, projection, vec![]));

        assert!(scan.children().is_empty());
        assert!(scan.expressions().is_empty());
        assert_eq!(scan.schema().fields().len(), 1);
    }
}

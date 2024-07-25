use std::fmt::Display;

use arrow::datatypes::SchemaRef;

use crate::expression::logical::expr::Expression;

use super::scan::Scan;

#[derive(Debug)]
pub enum LogicalPlan {
    Scan(Scan),
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan(plan) => plan.schema(),
        }
    }

    pub fn children(&self) -> &[&LogicalPlan] {
        match self {
            LogicalPlan::Scan(plan) => plan.children(),
        }
    }

    pub fn expressions(&self) -> &[&Expression] {
        match self {
            LogicalPlan::Scan(plan) => plan.expressions(),
        }
    }

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

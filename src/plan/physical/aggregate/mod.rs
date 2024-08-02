use std::{any::Any, fmt::Display, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};
use futures::StreamExt;
use no_groups::AggregateStream;
use row_hash::GroupedHashAggregateStream;

use crate::{
    error::Result,
    expression::physical::{aggregate::AggregateExpr, expr::PhysicalExpression},
    io::RecordBatchStream,
};

use super::plan::{format_exec, ExecutionPlan};

pub mod group_values;
pub mod no_groups;
pub mod row_hash;

enum StreamType {
    AggregateStream(AggregateStream),
    GroupedHash(GroupedHashAggregateStream),
}

impl From<StreamType> for RecordBatchStream {
    fn from(value: StreamType) -> Self {
        use StreamType::*;

        match value {
            AggregateStream(stream) => stream.boxed(),
            GroupedHash(stream) => stream.boxed(),
        }
    }
}

/// Represents an aggregate physical plan.
#[derive(Debug)]
pub struct AggregateExec {
    /// The input physical plan.
    input: Arc<dyn ExecutionPlan>,
    /// Group by expressions including alias.
    group_by: Vec<(Arc<dyn PhysicalExpression>, String)>,
    /// Aggregate expressions.
    aggregate_expressions: Vec<Arc<dyn AggregateExpr>>,
    /// The schema after the aggregate is applied.
    schema: SchemaRef,
}

impl AggregateExec {
    /// Creates a new [`AggregateExec`] instance.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_by: Vec<(Arc<dyn PhysicalExpression>, String)>,
        aggregate_expressions: Vec<Arc<dyn AggregateExpr>>,
    ) -> Result<Self> {
        let schema = Self::create_schema(input.as_ref(), &group_by, &aggregate_expressions)?;

        Ok(Self {
            input,
            group_by,
            aggregate_expressions,
            schema,
        })
    }

    /// Creates a new schema by combining the fields from
    /// the group by and aggregate expressions.
    fn create_schema(
        input: &dyn ExecutionPlan,
        group_by: &[(Arc<dyn PhysicalExpression>, String)],
        aggregate_expressions: &[Arc<dyn AggregateExpr>],
    ) -> Result<SchemaRef> {
        let input_schema = input.schema();
        let mut fields = Vec::with_capacity(group_by.len() + aggregate_expressions.len());
        for (expr, name) in group_by {
            let field = Field::new(
                name,
                expr.data_type(&input_schema)?,
                expr.nullable(&input_schema)?,
            );
            fields.push(field);
        }
        for expr in aggregate_expressions {
            fields.push(expr.field()?);
        }

        Ok(Arc::new(Schema::new(fields)))
    }
}

impl ExecutionPlan for AggregateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&dyn ExecutionPlan> {
        vec![self.input.as_ref()]
    }

    fn execute(&self) -> Result<RecordBatchStream> {
        let input = self.input.execute()?;

        let stream = if self.group_by.is_empty() {
            StreamType::AggregateStream(AggregateStream::try_new(
                input,
                self.schema(),
                self.aggregate_expressions.as_slice(),
            )?)
        } else {
            StreamType::GroupedHash(GroupedHashAggregateStream::try_new(
                input,
                self.schema(),
                self.group_by.clone(),
                self.aggregate_expressions.as_slice(),
            )?)
        };

        Ok(stream.into())
    }

    fn format(&self) -> String {
        format!(
            "AggregateExec: groupExprs:[{:?}], aggrExprs:[{:?}]",
            self.group_by, self.aggregate_expressions
        )
    }
}

impl Display for AggregateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_exec(self, f, 0)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::{
        expression::physical::{
            aggregate::count::CountExpr, column::ColumnExpr, expr::PhysicalExpression,
        },
        io::reader::csv::options::CsvFileOpenerConfig,
        plan::physical::{aggregate::AggregateExec, plan::ExecutionPlan, scan::csv::CsvExec},
        tests::create_schema,
    };

    #[tokio::test]
    async fn test_aggregate_count_with_grouping() {
        let schema = Arc::new(create_schema());
        let config = CsvFileOpenerConfig::new(schema.clone());
        let input = Arc::new(CsvExec::new("testdata/csv/simple.csv", config));
        let group_by: (Arc<dyn PhysicalExpression>, String) =
            (Arc::new(ColumnExpr::new("c1", 0)), "c1".to_string());
        let agg_exprs = Arc::new(CountExpr::new(Arc::new(ColumnExpr::new("c2", 1))));
        let exec = AggregateExec::try_new(input, vec![group_by], vec![agg_exprs]).unwrap();

        let mut stream = exec.execute().unwrap();

        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 6);
            assert_eq!(batch.num_columns(), 2);
        }
    }

    #[tokio::test]
    async fn test_aggregate_count() {
        let schema = Arc::new(create_schema());
        let config = CsvFileOpenerConfig::new(schema.clone());
        let input = Arc::new(CsvExec::new("testdata/csv/simple.csv", config));
        let exprs = Arc::new(CountExpr::new(Arc::new(ColumnExpr::new("c2", 0))));
        let exec = AggregateExec::try_new(input, vec![], vec![exprs]).unwrap();

        let mut stream = exec.execute().unwrap();

        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 1);
        }
    }
}

use std::{any::Any, fmt::Display, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};
use futures::StreamExt;
use hash::GroupedHashAggregateStream;
use no_groups::AggregateStream;

use crate::{
    error::Result,
    expression::physical::{aggregate::AggregateExpr, expr::PhysicalExpression},
    io::RecordBatchStream,
};

use super::plan::{format_exec, ExecutionPlan};

pub mod hash;
pub mod no_groups;

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
            StreamType::AggregateStream(AggregateStream::new(input, self.schema()))
        } else {
            StreamType::GroupedHash(GroupedHashAggregateStream {})
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

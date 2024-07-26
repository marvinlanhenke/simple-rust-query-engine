use std::{
    any::Any,
    fmt::Display,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::{Stream, StreamExt};

use crate::{error::Result, expression::physical::expr::PhysicalExpression, io::RecordBatchStream};

use super::plan::{format_exec, ExecutionPlan};

#[derive(Debug)]
pub struct ProjectionExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    expression: Vec<Arc<dyn PhysicalExpression>>,
}

impl ProjectionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        expression: Vec<Arc<dyn PhysicalExpression>>,
    ) -> Self {
        Self {
            input,
            schema,
            expression,
        }
    }
}

impl ExecutionPlan for ProjectionExec {
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
        let stream = ProjectionExecStream::new(input, self.schema.clone(), self.expression.clone());
        Ok(stream.boxed())
    }

    fn format(&self) -> String {
        format!("ProjectionExec: {:?}", self.expression)
    }
}

impl Display for ProjectionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_exec(self, f, 0)
    }
}

struct ProjectionExecStream {
    input: RecordBatchStream,
    schema: SchemaRef,
    expression: Vec<Arc<dyn PhysicalExpression>>,
}

impl ProjectionExecStream {
    pub fn new(
        input: RecordBatchStream,
        schema: SchemaRef,
        expression: Vec<Arc<dyn PhysicalExpression>>,
    ) -> Self {
        Self {
            input,
            schema,
            expression,
        }
    }

    fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        todo!()
    }
}

impl Stream for ProjectionExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|opt| match opt {
            Some(Ok(batch)) => Some(self.project_batch(&batch)),
            other => other,
        })
    }
}

use std::{
    any::Any,
    fmt::Display,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{RecordBatch, RecordBatchOptions},
    datatypes::SchemaRef,
};
use futures::{Stream, StreamExt};

use crate::{error::Result, expression::physical::expr::PhysicalExpression, io::RecordBatchStream};

use super::plan::{format_exec, ExecutionPlan};

/// Represents an [`ExecutionPlan`] for a projection operation.
#[derive(Debug)]
pub struct ProjectionExec {
    /// The input [`ExecutionPlan`].
    input: Arc<dyn ExecutionPlan>,
    /// A reference-counted schema of the projected data.
    schema: SchemaRef,
    /// A list of [`PhysicalExpression`] to apply for the projection.
    expression: Vec<Arc<dyn PhysicalExpression>>,
}

impl ProjectionExec {
    /// Creates a new [`ProjectionExec`] instance.
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

/// Represents a stream of projected [`RecordBatch`] instances.
struct ProjectionExecStream {
    /// The input stream of [`RecordBatch`] instances.
    input: RecordBatchStream,
    /// The schema of the projected data.
    schema: SchemaRef,
    /// A list of [`PhysicalExpression`] to apply for the projection.
    expression: Vec<Arc<dyn PhysicalExpression>>,
}

impl ProjectionExecStream {
    /// Creates a new [`ProjectionExecStream`] instance.
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

    /// Projects the expressions onto the given `RecordBatch`.
    fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .expression
            .iter()
            .map(|expr| {
                expr.eval(batch)
                    .and_then(|res| res.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        if columns.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            return Ok(RecordBatch::try_new_with_options(
                self.schema.clone(),
                columns,
                &options,
            )?);
        }

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::{
        expression::physical::column::ColumnExpr,
        io::reader::csv::options::CsvFileOpenerConfig,
        plan::physical::{plan::ExecutionPlan, projection::ProjectionExec, scan::csv::CsvExec},
        tests::create_schema,
    };

    #[tokio::test]
    async fn test_projection_stream() {
        let schema = Arc::new(create_schema());
        let config = CsvFileOpenerConfig::builder(schema.clone()).build();
        let input = Arc::new(CsvExec::new("testdata/csv/simple.csv", config));
        let exec = ProjectionExec::new(input, schema, vec![Arc::new(ColumnExpr::new("a", 0))]);

        let mut stream = exec.execute().unwrap();

        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 6);
            assert_eq!(batch.num_columns(), 1);
        }
    }
}

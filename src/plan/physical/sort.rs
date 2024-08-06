use std::{any::Any, fmt::Display, sync::Arc};

use arrow::compute::SortColumn;
use arrow_array::RecordBatch;
use arrow_schema::{SchemaRef, SortOptions};
use futures::{Stream, StreamExt};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{physical::expr::PhysicalExpression, values::ColumnarValue},
    io::RecordBatchStream,
};

use super::plan::{format_exec, ExecutionPlan};

/// Represents a sort execution plan.
#[derive(Debug)]
pub struct SortExec {
    /// The input [`ExecutionPlan`].
    input: Arc<dyn ExecutionPlan>,
    /// The sort expression.
    expression: Vec<Arc<dyn PhysicalExpression>>,
    /// The sort options.
    options: SortOptions,
}

impl SortExec {
    /// Creates a new [`SortExec`] instance.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        expression: Vec<Arc<dyn PhysicalExpression>>,
        options: SortOptions,
    ) -> Self {
        Self {
            input,
            expression,
            options,
        }
    }

    /// Evaluates the sort expressions and creates a list of [`SortColumn`] instances.
    fn map_to_sort_column(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.expression
            .iter()
            .map(|e| {
                e.eval(batch).and_then(|v| match v {
                    ColumnarValue::Array(values) => Ok(SortColumn {
                        values,
                        options: Some(self.options),
                    }),
                    other => Err(Error::InvalidOperation {
                        message: format!("Converting {:?} into SortColumn is not supported", other),
                        location: location!(),
                    }),
                })
            })
            .collect()
    }
}

impl ExecutionPlan for SortExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&dyn ExecutionPlan> {
        vec![self.input.as_ref()]
    }

    fn execute(&self) -> Result<RecordBatchStream> {
        todo!()
    }

    fn format(&self) -> String {
        let expr_str = self
            .expression
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("SortExec: [{}]", expr_str)
    }
}

impl Display for SortExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_exec(self, f, 0)
    }
}

struct SortExecStream {
    input: RecordBatchStream,
    sort_columns: Vec<SortColumn>,
}

// TODO: impl streaming merge sort
impl SortExecStream {
    fn new(input: RecordBatchStream, sort_columns: Vec<SortColumn>) -> Self {
        Self {
            input,
            sort_columns,
        }
    }
}

impl Stream for SortExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|opt| match opt {
            Some(Ok(_batch)) => todo!(),
            other => other,
        })
    }
}

#[cfg(test)]
mod tests {}

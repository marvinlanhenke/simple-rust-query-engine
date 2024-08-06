use std::{any::Any, fmt::Display, sync::Arc};

use arrow::compute::{lexsort_to_indices, take, SortColumn};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{SchemaRef, SortOptions};
use futures::{StreamExt, TryStreamExt};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{physical::expr::PhysicalExpression, values::ColumnarValue},
    io::RecordBatchStream,
    plan::physical::{
        plan::{format_exec, ExecutionPlan},
        sorts::stream::RowCursorStream,
    },
};

use super::stream::EmptyRecordBatchStream;

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
        let mut input = self.input.execute()?;

        let mut sorter = Sorter::new(self.input.schema(), self.expression.clone(), self.options);

        let stream = futures::stream::once(async move {
            while let Some(Ok(batch)) = input.next().await {
                sorter.insert_batch(batch);
            }
            sorter.sort()
        })
        .try_flatten();

        Ok(stream.boxed())
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

struct Sorter {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    batches_sorted: bool,
    expression: Vec<Arc<dyn PhysicalExpression>>,
    options: SortOptions,
}

impl Sorter {
    fn new(
        schema: SchemaRef,
        expression: Vec<Arc<dyn PhysicalExpression>>,
        options: SortOptions,
    ) -> Self {
        Self {
            schema,
            batches: vec![],
            batches_sorted: false,
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

    fn insert_batch(&mut self, batch: RecordBatch) {
        if batch.num_rows() > 0 {
            self.batches.push(batch);
            self.batches_sorted = false;
        }
    }

    fn sort(&mut self) -> Result<RecordBatchStream> {
        if self.batches.is_empty() {
            let stream = EmptyRecordBatchStream;
            return Ok(stream.boxed());
        }

        self.in_memory_stream_sort()
    }

    fn in_memory_stream_sort(&mut self) -> Result<RecordBatchStream> {
        if self.batches.len() == 1 {
            let batch = self.batches.remove(0);
            return self.in_memory_batch_sort(batch);
        }

        let streams = std::mem::take(&mut self.batches)
            .into_iter()
            .map(|batch| self.in_memory_batch_sort(batch))
            .collect::<Result<Vec<_>>>()?;

        self.merge_streams(streams)
    }

    fn in_memory_batch_sort(&self, batch: RecordBatch) -> Result<RecordBatchStream> {
        let sort_columns = self.map_to_sort_column(&batch)?;

        let stream = futures::stream::once(async move {
            let indices = lexsort_to_indices(&sort_columns, None)?;
            let columns = batch
                .columns()
                .iter()
                .map(|col| take(col.as_ref(), &indices, None).map_err(Error::from))
                .collect::<Result<Vec<ArrayRef>>>()?;
            let options = RecordBatchOptions::new().with_row_count(Some(indices.len()));

            Ok(RecordBatch::try_new_with_options(
                batch.schema(),
                columns,
                &options,
            )?)
        });

        Ok(stream.boxed())
    }

    fn merge_streams(&self, streams: Vec<RecordBatchStream>) -> Result<RecordBatchStream> {
        let _streams =
            RowCursorStream::try_new(&self.schema, self.expression.clone(), streams, self.options)?;
        // let merge_streams = MergeStream::new();
        // Ok(merge_streams.boxed())
        todo!()
    }
}

#[cfg(test)]
mod tests {}
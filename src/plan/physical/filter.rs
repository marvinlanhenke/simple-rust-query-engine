use std::{any::Any, fmt::Display, sync::Arc};

use arrow::{
    array::{BooleanArray, RecordBatch},
    compute::filter_record_batch,
    datatypes::{DataType, SchemaRef},
};
use futures::{Stream, StreamExt};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::physical::expr::PhysicalExpression,
    io::RecordBatchStream,
};

use super::plan::{format_exec, ExecutionPlan};

#[derive(Debug)]
pub struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<dyn PhysicalExpression>,
}

impl FilterExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<dyn PhysicalExpression>,
    ) -> Result<Self> {
        if predicate.data_type(&input.schema())? != DataType::Boolean {
            return Err(Error::InvalidData {
                message: format!(
                    "Cannot create filter with non-boolean predicate '{}'",
                    predicate
                ),
                location: location!(),
            });
        };

        Ok(Self { input, predicate })
    }
}

impl ExecutionPlan for FilterExec {
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
        let input = self.input.execute()?;
        let stream = FilterExecStream::new(input, self.predicate.clone());

        Ok(stream.boxed())
    }

    fn format(&self) -> String {
        format!("FilterExec: [{}]", self.predicate)
    }
}

impl Display for FilterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_exec(self, f, 0)
    }
}

struct FilterExecStream {
    input: RecordBatchStream,
    predicate: Arc<dyn PhysicalExpression>,
}

impl FilterExecStream {
    fn new(input: RecordBatchStream, predicate: Arc<dyn PhysicalExpression>) -> Self {
        Self { input, predicate }
    }

    fn filter_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.predicate
            .eval(batch)
            .and_then(|val| val.into_array(batch.num_rows()))
            .and_then(|arr| {
                let predicate_arr =
                    arr.as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| Error::Arrow {
                            message: "Failed to downcast ArrowArray".to_string(),
                            location: location!(),
                        })?;
                Ok(filter_record_batch(batch, predicate_arr)?)
            })
    }
}

impl Stream for FilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|opt| match opt {
            Some(Ok(batch)) => Some(self.filter_batch(&batch)),
            other => other,
        })
    }
}

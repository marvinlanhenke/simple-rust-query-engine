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

/// Represents a filter execution plan in a query.
#[derive(Debug)]
pub struct FilterExec {
    /// The input [`ExecutionPlan`].
    input: Arc<dyn ExecutionPlan>,
    /// The predicate expression used to filter rows.
    predicate: Arc<dyn PhysicalExpression>,
}

impl FilterExec {
    /// Attempts to create a new [`FilterExec`] instance from specified (boolean) `predicate`.
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

/// Represents a stream of filtered [`RecordBatch`] instances.
struct FilterExecStream {
    /// The input stream of `RecordBatch`'es.
    input: RecordBatchStream,
    /// The predicate expression used to filter rows.
    predicate: Arc<dyn PhysicalExpression>,
}

impl FilterExecStream {
    /// Creates a new [`FilterExecStream`] instance.
    fn new(input: RecordBatchStream, predicate: Arc<dyn PhysicalExpression>) -> Self {
        Self { input, predicate }
    }

    /// Filters a [`RecordBatch`] based on the predicate expression.
    ///
    /// Evaluates the predicate to produce a `BooleanArray`, which is then used
    /// to filter and return rows that match the predicate.
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::{
        expression::{
            operator::Operator,
            physical::{binary::BinaryExpr, column::ColumnExpr, literal::LiteralExpr},
            values::ScalarValue,
        },
        io::reader::csv::options::CsvFileOpenerConfig,
        plan::physical::{filter::FilterExec, plan::ExecutionPlan, scan::csv::CsvExec},
        tests::create_schema,
    };

    #[tokio::test]
    async fn test_filter_stream() {
        let schema = Arc::new(create_schema());
        let config = CsvFileOpenerConfig::builder(schema.clone()).build();
        let input = Arc::new(CsvExec::new("testdata/csv/simple.csv", config));
        let lhs = Arc::new(ColumnExpr::new("a", 0));
        let rhs = Arc::new(LiteralExpr::new(ScalarValue::Utf8(Some("a".to_string()))));
        let predicate = Arc::new(BinaryExpr::new(lhs, Operator::Eq, rhs));
        let exec = FilterExec::try_new(input, predicate).unwrap();

        let mut stream = exec.execute().unwrap();

        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 3);
        }
    }
}

use std::sync::Arc;

use crate::{
    error::Result,
    expression::physical::{
        aggregate::{Accumulator, AggregateExpr},
        column::ColumnExpr,
        expr::PhysicalExpression,
    },
};
use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use futures::{Stream, StreamExt};

use crate::io::RecordBatchStream;

struct AggregateStreamInner {
    input: RecordBatchStream,
    schema: SchemaRef,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpression>>>,
    accumulators: Vec<Box<dyn Accumulator>>,
    finished: bool,
}
impl AggregateStreamInner {
    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.accumulators
            .iter_mut()
            .zip(self.aggregate_expressions.iter())
            .try_for_each(|(accu, expr)| {
                let values = &expr
                    .iter()
                    .map(|e| e.eval(batch).and_then(|v| v.into_array(batch.num_rows())))
                    .collect::<Result<Vec<_>>>()?;
                accu.update_batch(values)
            })
    }

    fn finalize_aggregation(&mut self) -> Result<Vec<ArrayRef>> {
        self.accumulators
            .iter_mut()
            .map(|acc| acc.eval().map(|v| v.to_array(1)))
            .collect()
    }
}

pub struct AggregateStream {
    inner: RecordBatchStream,
}

impl AggregateStream {
    pub fn try_new(
        input: RecordBatchStream,
        schema: SchemaRef,
        aggregate_exprs: &[Arc<dyn AggregateExpr>],
    ) -> Result<Self> {
        let aggregate_expressions = Self::create_aggregate_expressions(aggregate_exprs)?;
        let accumulators = Self::create_accumulators(aggregate_exprs)?;

        let inner = AggregateStreamInner {
            input,
            schema,
            aggregate_expressions,
            accumulators,
            finished: false,
        };

        let stream = futures::stream::unfold(inner, |mut state| async move {
            if state.finished {
                return None;
            }

            loop {
                let result = match state.input.next().await {
                    Some(Ok(batch)) => match state.aggregate_batch(&batch) {
                        Ok(_) => continue,
                        Err(e) => Err(e),
                    },
                    Some(Err(e)) => Err(e),
                    None => state.finalize_aggregation().and_then(|columns| {
                        RecordBatch::try_new(state.schema.clone(), columns).map_err(Into::into)
                    }),
                };
                state.finished = true;
                return Some((result, state));
            }
        });
        let stream = stream.fuse();
        let stream = stream.boxed();

        Ok(Self { inner: stream })
    }

    fn create_accumulators(
        aggregate_exprs: &[Arc<dyn AggregateExpr>],
    ) -> Result<Vec<Box<dyn Accumulator>>> {
        aggregate_exprs
            .iter()
            .map(|expr| expr.create_accumulator())
            .collect()
    }

    fn create_aggregate_expressions(
        aggregate_exprs: &[Arc<dyn AggregateExpr>],
    ) -> Result<Vec<Vec<Arc<dyn PhysicalExpression>>>> {
        let mut base_idx = 0;
        aggregate_exprs
            .iter()
            .map(|expr| {
                let exprs = Self::merge_expressions(base_idx, expr)?;
                base_idx += exprs.len();
                Ok(exprs)
            })
            .collect()
    }

    fn merge_expressions(
        base_idx: usize,
        expr: &Arc<dyn AggregateExpr>,
    ) -> Result<Vec<Arc<dyn PhysicalExpression>>> {
        expr.state_fields().map(|fields| {
            fields
                .iter()
                .enumerate()
                .map(|(idx, f)| Arc::new(ColumnExpr::new(f.name(), base_idx + idx)) as _)
                .collect()
        })
    }
}

impl Stream for AggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;

    use crate::{
        expression::physical::{aggregate::count::CountExpr, column::ColumnExpr},
        io::reader::csv::options::CsvFileOpenerConfig,
        plan::physical::{
            aggregate::no_groups::AggregateStream, plan::ExecutionPlan, scan::csv::CsvExec,
        },
        tests::create_schema,
    };

    #[tokio::test]
    async fn test_drive() {
        let schema = Arc::new(create_schema());
        let config = CsvFileOpenerConfig::new(schema.clone());
        let scan = CsvExec::new("testdata/csv/simple.csv", config);
        let input = scan.execute().unwrap();
        let expr = Arc::new(CountExpr::new(Arc::new(ColumnExpr::new("c1", 0))));

        let agg_schema = Arc::new(Schema::new(vec![Field::new(
            "COUNT",
            DataType::Int64,
            true,
        )]));
        let mut stream = AggregateStream::try_new(input, agg_schema, &[expr]).unwrap();

        while let Some(batch) = stream.next().await {
            println!("{:?}", batch);
        }
    }
}

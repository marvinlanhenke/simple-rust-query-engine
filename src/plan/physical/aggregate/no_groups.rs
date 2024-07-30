use std::sync::Arc;

use crate::{
    error::Result,
    expression::physical::{
        aggregate::{Accumulator, AggregateExpr},
        expr::PhysicalExpression,
    },
};
use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use futures::{Stream, StreamExt};

use crate::io::RecordBatchStream;

/// Represents the inner state for an [`AggregateStream`].
struct AggregateStreamInner {
    /// The input stream of `RecordBatch`'es
    input: RecordBatchStream,
    /// The schema after the aggregation.
    schema: SchemaRef,
    /// The aggregate expressions to be evaluated.
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpression>>>,
    /// The accumulator used to maintain the aggregation state.
    accumulators: Vec<Box<dyn Accumulator>>,
    /// Indicates whether the aggregation is finished.
    finished: bool,
}
impl AggregateStreamInner {
    /// Updates the accumulators with values from a given `RecordBatch`.
    ///
    /// This method iterates pairwise over expression and accumulator,
    /// evaluates the expression against the current `RecordBatch`, converts the results into arrays,
    /// and uses these arrays to update the corresponding accumulator.
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

    /// Finalizes the aggregation and returns the result.
    ///
    /// This method evaluates the accumulators and returns the final aggregated values as an array.
    fn finalize_aggregation(&mut self) -> Result<Vec<ArrayRef>> {
        self.accumulators
            .iter_mut()
            .map(|acc| acc.eval().map(|v| v.to_array(1)))
            .collect()
    }
}

/// Represents an aggregate stream with no groupings.
pub struct AggregateStream {
    /// The inner stream of `RecordBatch`'es.
    inner: RecordBatchStream,
}

impl AggregateStream {
    /// Attempts to create a new [`AggregateStream`] instance.
    pub fn try_new(
        input: RecordBatchStream,
        schema: SchemaRef,
        aggregate_exprs: &[Arc<dyn AggregateExpr>],
    ) -> Result<Self> {
        let aggregate_expressions = Self::create_aggregate_expressions(aggregate_exprs);
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

    /// Creates the accumulators for the aggregate expressions.
    fn create_accumulators(
        aggregate_exprs: &[Arc<dyn AggregateExpr>],
    ) -> Result<Vec<Box<dyn Accumulator>>> {
        aggregate_exprs
            .iter()
            .map(|expr| expr.create_accumulator())
            .collect()
    }

    /// Creates the aggregate expressions by collecting the underlying `PhysicalExpression`s.
    fn create_aggregate_expressions(
        aggregate_exprs: &[Arc<dyn AggregateExpr>],
    ) -> Vec<Vec<Arc<dyn PhysicalExpression>>> {
        aggregate_exprs
            .iter()
            .map(|agg| agg.expressions())
            .collect()
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

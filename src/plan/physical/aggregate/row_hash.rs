use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow_array::ArrayRef;
use arrow_schema::Schema;
use futures::{Stream, StreamExt};

use crate::{
    error::Result,
    expression::physical::{
        aggregate::{AggregateExpr, GroupAccumulator},
        expr::PhysicalExpression,
    },
    io::RecordBatchStream,
};

use super::group_values::GroupValues;

struct GroupedHashAggregateStreamInner {
    input: RecordBatchStream,
    schema: SchemaRef,
    group_by: Vec<(Arc<dyn PhysicalExpression>, String)>,
    group_values: GroupValues,
    current_group_indices: Vec<usize>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpression>>>,
    accumulators: Vec<Box<dyn GroupAccumulator>>,
    finished: bool,
}

impl GroupedHashAggregateStreamInner {
    fn group_aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let group_by_values = self
            .group_by
            .iter()
            .map(|(expr, _)| {
                expr.eval(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        let aggregation_values = self
            .aggregate_expressions
            .iter()
            .map(|exprs| {
                exprs
                    .iter()
                    .map(|expr| {
                        expr.eval(batch)
                            .and_then(|v| v.into_array(batch.num_rows()))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        self.group_values
            .intern(group_by_values.as_slice(), &mut self.current_group_indices)?;
        let group_indices = &self.current_group_indices;
        let total_num_groups = self.group_values.len();

        self.accumulators
            .iter_mut()
            .zip(aggregation_values.iter())
            .try_for_each(|(accu, values)| {
                accu.update_batch(values, group_indices, total_num_groups)
            })
    }

    fn finalize_aggregation(&mut self) -> Result<Vec<ArrayRef>> {
        let mut output = self.group_values.emit()?;

        for accu in self.accumulators.iter_mut() {
            output.extend(accu.state()?);
        }

        Ok(output)
    }
}

pub struct GroupedHashAggregateStream {
    inner: RecordBatchStream,
}

impl GroupedHashAggregateStream {
    pub fn try_new(
        input: RecordBatchStream,
        schema: SchemaRef,
        group_by: Vec<(Arc<dyn PhysicalExpression>, String)>,
        aggregate_exprs: &[Arc<dyn AggregateExpr>],
    ) -> Result<Self> {
        let aggregate_expressions = Self::create_aggregate_expressions(aggregate_exprs);
        let accumulators = Self::create_accumulators(aggregate_exprs)?;
        let group_schema = Self::group_schema(&schema, group_by.len());
        let group_values = GroupValues::try_new(group_schema)?;

        let inner = GroupedHashAggregateStreamInner {
            input,
            schema,
            group_by,
            group_values,
            current_group_indices: vec![],
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
                    Some(Ok(batch)) => match state.group_aggregate_batch(&batch) {
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

    fn group_schema(schema: &Schema, group_count: usize) -> SchemaRef {
        let group_fields = schema.fields()[0..group_count].to_vec();
        Arc::new(Schema::new(group_fields))
    }

    /// Creates the group accumulators for the aggregate expressions.
    fn create_accumulators(
        aggregate_exprs: &[Arc<dyn AggregateExpr>],
    ) -> Result<Vec<Box<dyn GroupAccumulator>>> {
        aggregate_exprs
            .iter()
            .map(|expr| expr.create_group_accumulator())
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

impl Stream for GroupedHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::Stream;

use crate::{
    error::Result,
    expression::physical::{aggregate::GroupAccumulator, expr::PhysicalExpression},
    io::RecordBatchStream,
};

struct GroupedHashAggregateStreamInner {
    input: RecordBatchStream,
    schema: SchemaRef,
    group_by: Vec<(Arc<dyn PhysicalExpression>, String)>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpression>>>,
    accumulators: Vec<Box<dyn GroupAccumulator>>,
    finished: bool,
}

pub struct GroupedHashAggregateStream {
    inner: RecordBatchStream,
}

impl Stream for GroupedHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

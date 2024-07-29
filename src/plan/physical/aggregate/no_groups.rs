use crate::error::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::Stream;

use crate::io::RecordBatchStream;

pub struct AggregateStream {
    input: RecordBatchStream,
    schema: SchemaRef,
}

impl AggregateStream {
    pub fn new(input: RecordBatchStream, schema: SchemaRef) -> Self {
        Self { input, schema }
    }
}

impl Stream for AggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SortOptions};
use futures::{ready, stream::Fuse, Stream, StreamExt};

use crate::{error::Result, expression::physical::expr::PhysicalExpression, io::RecordBatchStream};

struct FusedStreams(Vec<Fuse<RecordBatchStream>>);

impl FusedStreams {
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match ready!(self.0[stream_idx].poll_next_unpin(cx)) {
                Some(Ok(batch)) if batch.num_rows() == 0 => continue,
                other => return Poll::Ready(other),
            }
        }
    }
}

pub struct RowCursorStream {
    converter: RowConverter,
    expression: Vec<Arc<dyn PhysicalExpression>>,
    streams: FusedStreams,
}

impl RowCursorStream {
    pub fn try_new(
        schema: &Schema,
        expression: Vec<Arc<dyn PhysicalExpression>>,
        streams: Vec<RecordBatchStream>,
        options: SortOptions,
    ) -> Result<Self> {
        let sort_fields = expression
            .iter()
            .map(|expr| {
                let data_type = expr.data_type(schema)?;
                Ok(SortField::new_with_options(data_type, options))
            })
            .collect::<Result<Vec<_>>>()?;
        let streams = streams.into_iter().map(|stream| stream.fuse()).collect();
        let streams = FusedStreams(streams);
        let converter = RowConverter::new(sort_fields)?;

        Ok(Self {
            converter,
            expression,
            streams,
        })
    }

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<(Rows, RecordBatch)>>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|res| {
            res.and_then(|batch| {
                let rows = self.convert_batch(&batch)?;
                Ok((rows, batch))
            })
        }))
    }

    fn convert_batch(&self, batch: &RecordBatch) -> Result<Rows> {
        let cols = self
            .expression
            .iter()
            .map(|expr| expr.eval(batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        Ok(self.converter.convert_columns(&cols)?)
    }
}

pub struct MergeStream {
    streams: RowCursorStream,
    aborted: bool,
    loser_tree: Vec<usize>,
    loser_tree_adjusted: bool,
}

impl MergeStream {
    pub fn new() -> Self {
        todo!()
    }

    fn maybe_poll_stream(&mut self, _cx: &mut Context<'_>, _idx: usize) -> Result<()> {
        todo!()
    }

    fn poll_next_inner(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }
        todo!()
    }
}

impl Stream for MergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
    }
}

/// An empty [`RecordBatchStream`] that produces no results.
pub struct EmptyRecordBatchStream;

impl Stream for EmptyRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

#[cfg(test)]
mod tests {}

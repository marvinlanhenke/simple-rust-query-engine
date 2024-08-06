use std::{
    sync::Arc,
    task::{Context, Poll},
};

use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef, SortOptions};
use futures::{ready, stream::Fuse, Stream, StreamExt};

use crate::{error::Result, expression::physical::expr::PhysicalExpression, io::RecordBatchStream};

use super::{builder::BatchBuilder, cursor::Cursor};

const DEFAULT_BATCH_SIZE: usize = 1024;

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
    ) -> Result<Self> {
        let sort_fields = expression
            .iter()
            .map(|expr| {
                let options = match expr.ascending() {
                    Some(asc) => SortOptions {
                        descending: !asc,
                        ..Default::default()
                    },
                    None => SortOptions::default(),
                };
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

    pub fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
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
    in_progress: BatchBuilder,
    streams: RowCursorStream,
    aborted: bool,
    cursors: Vec<Option<Cursor>>,
    loser_tree: Vec<usize>,
    loser_tree_adjusted: bool,
    batch_size: usize,
    produced: usize,
}

impl MergeStream {
    pub fn new(streams: RowCursorStream, schema: SchemaRef) -> Self {
        let stream_count = streams.partitions();

        Self {
            in_progress: BatchBuilder::new(schema, stream_count, DEFAULT_BATCH_SIZE),
            streams,
            aborted: false,
            cursors: (0..stream_count).map(|_| None).collect(),
            loser_tree: vec![],
            loser_tree_adjusted: false,
            batch_size: DEFAULT_BATCH_SIZE,
            produced: 0,
        }
    }

    fn maybe_poll_stream(&mut self, cx: &mut Context<'_>, idx: usize) -> Poll<Result<()>> {
        if self.cursors[idx].is_some() {
            // Cursor not finished - no need for a new RecordBatch.
            return Poll::Ready(Ok(()));
        }

        match ready!(self.streams.poll_next(cx, idx)) {
            None => Poll::Ready(Ok(())),
            Some(Err(e)) => Poll::Ready(Err(e)),
            Some(Ok((rows, batch))) => {
                self.cursors[idx] = Some(Cursor::new(rows));
                self.in_progress.push_batch(idx, batch);
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        if self.loser_tree.is_empty() {
            for idx in 0..self.streams.partitions() {
                if let Err(e) = ready!(self.maybe_poll_stream(cx, idx)) {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
            self.init_loser_tree();
        }

        loop {
            if !self.loser_tree_adjusted {
                let winner = self.loser_tree[0];
                if let Err(e) = ready!(self.maybe_poll_stream(cx, winner)) {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
                self.update_loser_tree();
            }

            let stream_idx = self.loser_tree[0];
            if self.advance(stream_idx) {
                self.loser_tree_adjusted = false;
                self.in_progress.push_row(stream_idx);

                if self.in_progress.len() < self.batch_size {
                    continue;
                }
            }

            self.produced += self.in_progress.len();
            return Poll::Ready(self.in_progress.build_record_batch().transpose());
        }
    }

    fn advance(&mut self, stream_idx: usize) -> bool {
        let slot = &mut self.cursors[stream_idx];
        match slot.as_mut() {
            Some(c) => {
                c.advance();
                if c.is_finished() {
                    *slot = None;
                }
                true
            }
            None => false,
        }
    }

    #[inline]
    fn is_gt(&self, lhs: usize, rhs: usize) -> bool {
        match (&self.cursors[lhs], &self.cursors[rhs]) {
            (None, _) => true,
            (_, None) => false,
            (Some(lc), Some(rc)) => lc.cmp(rc).then_with(|| lhs.cmp(&rhs)).is_gt(),
        }
    }

    #[inline]
    fn lt_leaf_node_index(&self, cursor_index: usize) -> usize {
        (self.cursors.len() + cursor_index) / 2
    }

    #[inline]
    fn lt_parent_node_index(&self, node_index: usize) -> usize {
        node_index / 2
    }

    fn init_loser_tree(&mut self) {
        self.loser_tree = vec![usize::MAX; self.cursors.len()];

        for idx in 0..self.cursors.len() {
            let mut winner = idx;
            let mut compare_to = self.lt_leaf_node_index(idx);
            while compare_to != 0 && self.loser_tree[compare_to] != usize::MAX {
                let challenger = self.loser_tree[compare_to];
                if self.is_gt(winner, challenger) {
                    self.loser_tree[compare_to] = winner;
                    winner = challenger;
                }
                compare_to = self.lt_parent_node_index(compare_to);
            }
            self.loser_tree[compare_to] = winner;
        }
        self.loser_tree_adjusted = true;
    }

    fn update_loser_tree(&mut self) {
        let mut winner = self.loser_tree[0];
        let mut compare_to = self.lt_leaf_node_index(winner);
        while compare_to != 0 {
            let challenger = self.loser_tree[compare_to];
            if self.is_gt(winner, challenger) {
                self.loser_tree[compare_to] = winner;
                winner = challenger;
            }
            compare_to = self.lt_parent_node_index(compare_to);
        }
        self.loser_tree[0] = winner;
        self.loser_tree_adjusted = true;
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

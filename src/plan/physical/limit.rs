use std::{any::Any, cmp::Ordering, fmt::Display, sync::Arc, task::Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{Stream, StreamExt};

use crate::{error::Result, io::RecordBatchStream};

use super::plan::{format_exec, ExecutionPlan};

/// Represents a physical execution plan node that limits the number of rows processed
/// in a query execution, optionally skipping a number of rows and fetching a maximum
/// number of rows from the input execution plan.
#[derive(Debug)]
pub struct LimitExec {
    /// The input [`ExecutionPlan`].
    input: Arc<dyn ExecutionPlan>,
    /// The number of rows to skip before fetching starts.
    skip: usize,
    /// The maximum number of rows to fetch after skipping.
    /// If `None`, all rows after the skip will be fetched.
    fetch: Option<usize>,
}

impl LimitExec {
    /// Creates a new [`LimitExec`] instance.
    pub fn new(input: Arc<dyn ExecutionPlan>, skip: usize, fetch: Option<usize>) -> Self {
        Self { input, skip, fetch }
    }
}

impl ExecutionPlan for LimitExec {
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
        let schema = self.input.schema();
        let input = self.input.execute()?;
        let stream = LimitExecStream::new(input, schema, self.skip, self.fetch);

        Ok(stream.boxed())
    }

    fn format(&self) -> String {
        format!("LimitExec: [skip: {}, fetch: {:?}]", self.skip, self.fetch)
    }
}

impl Display for LimitExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_exec(self, f, 0)
    }
}

/// Represents a stream that applies the skip and fetch limits
/// to the rows, produced by the input execution plan.
///
/// The `LimitExecStream` handles the logic of skipping a specified number of rows
/// and then fetching up to a maximum number of rows from the input stream.
struct LimitExecStream {
    /// The input stream of `RecordBatch`es.
    input: Option<RecordBatchStream>,
    /// The schema of the output `RecordBatch`.
    schema: SchemaRef,
    /// The number of rows to skip before fetching starts.
    skip: usize,
    /// The maximum number of rows to fetch after skipping.
    fetch: usize,
}

impl LimitExecStream {
    /// Creates a new [`LimitExecStream`] instance.
    fn new(input: RecordBatchStream, schema: SchemaRef, skip: usize, fetch: Option<usize>) -> Self {
        Self {
            input: Some(input),
            schema,
            skip,
            fetch: fetch.unwrap_or(0),
        }
    }

    /// Polls the input stream and skips the specified number of rows.
    ///
    /// This method continues polling the input stream until the required number of rows
    /// have been skipped. If the current `RecordBatch` contains fewer rows than needed
    /// to fulfill the skip requirement, it will be completely skipped. Otherwise, the
    /// remaining rows after the skip are returned.
    fn poll_and_skip(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let input = self.input.as_mut().expect("input stream is not set");

        loop {
            let poll = input.poll_next_unpin(cx).map_ok(|batch| {
                if batch.num_rows() <= self.skip {
                    self.skip -= batch.num_rows();
                    RecordBatch::new_empty(self.schema.clone())
                } else {
                    let offset = self.skip;
                    let rows = batch.num_rows() - self.skip;
                    self.skip = 0;
                    batch.slice(offset, rows)
                }
            });

            match &poll {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() > 0 {
                        break poll;
                    } else {
                        continue;
                    }
                }
                Poll::Ready(Some(Err(_))) | Poll::Ready(None) | Poll::Pending => break poll,
            }
        }
    }

    /// Limits the rows of the `RecordBatch` according to the fetch limit.
    ///
    /// If the `RecordBatch` contains fewer rows than the fetch limit, the entire batch
    /// is returned. If it contains more rows, only the number of rows up to the fetch limit
    /// are returned, and the fetch limit is reduced to zero.
    fn stream_limit(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        if self.fetch == 0 {
            self.input = None;
            return None;
        }

        match batch.num_rows().cmp(&self.fetch) {
            Ordering::Less => {
                self.fetch -= batch.num_rows();
                Some(batch)
            }
            Ordering::Greater | Ordering::Equal => {
                let remaining = self.fetch;
                self.fetch = 0;
                self.input = None;
                Some(batch.slice(0, remaining))
            }
        }
    }
}

impl Stream for LimitExecStream {
    type Item = Result<RecordBatch>;

    /// Polls the next `RecordBatch` from the `LimitExecStream`.
    ///
    /// This method manages the logic of skipping rows and applying the fetch limit.
    /// If skipping is required, it delegates to `poll_and_skip`. If the fetch limit
    /// has been reached, it terminates the stream.
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let has_started_fetching = self.skip == 0;

        match &mut self.input {
            Some(input) => {
                let poll = if has_started_fetching {
                    input.poll_next_unpin(cx)
                } else {
                    self.poll_and_skip(cx)
                };

                poll.map(|opt_res| match opt_res {
                    Some(Ok(batch)) => Ok(self.stream_limit(batch)).transpose(),
                    other => other,
                })
            }
            None => Poll::Ready(None),
        }
    }
}

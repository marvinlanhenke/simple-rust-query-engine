use std::{any::Any, cmp::Ordering, fmt::Display, sync::Arc, task::Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{Stream, StreamExt};

use crate::{error::Result, io::RecordBatchStream};

use super::plan::{format_exec, ExecutionPlan};

#[derive(Debug)]
pub struct LimitExec {
    input: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: Option<usize>,
}

impl LimitExec {
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

struct LimitExecStream {
    input: Option<RecordBatchStream>,
    schema: SchemaRef,
    skip: usize,
    fetch: usize,
}

impl LimitExecStream {
    fn new(input: RecordBatchStream, schema: SchemaRef, skip: usize, fetch: Option<usize>) -> Self {
        Self {
            input: Some(input),
            schema,
            skip,
            fetch: fetch.unwrap_or(0),
        }
    }

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

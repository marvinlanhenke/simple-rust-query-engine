use std::{
    any::Any,
    fmt::{Debug, Display},
    sync::Arc,
    task::Poll,
};

use arrow::{array::BooleanBufferBuilder, compute::concat_batches};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt, TryStreamExt};
use snafu::location;

use crate::{
    error::{Error, Result},
    io::RecordBatchStream,
    plan::{
        logical::join::JoinType,
        physical::{
            joins::utils::{create_join_schema, is_valid_join},
            plan::{format_exec, ExecutionPlan},
        },
    },
};

use super::utils::{JoinColumnIndex, JoinFilter};

type InnerTableFuture = BoxFuture<'static, Result<InnerTableData>>;

#[derive(Debug)]
struct InnerTableData {
    /// The inner table's `RecordBatch`.
    batch: RecordBatch,
    /// A bitmap builder for visited left indices.
    visited: BooleanBufferBuilder,
}

impl InnerTableData {
    fn new(batch: RecordBatch, visited: BooleanBufferBuilder) -> Self {
        Self { batch, visited }
    }

    fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    fn visited(&self) -> &BooleanBufferBuilder {
        &self.visited
    }
}

enum InnerTableState {
    Initial(InnerTableFuture),
    Ready(InnerTableData),
}

impl InnerTableState {
    fn try_as_init_mut(&mut self) -> Result<&mut InnerTableFuture> {
        match self {
            InnerTableState::Initial(fut) => Ok(fut),
            _ => Err(Error::InvalidOperation {
                message: "Expected build side in initial state".to_string(),
                location: location!(),
            }),
        }
    }

    fn try_as_ready_mut(&mut self) -> Result<&mut InnerTableData> {
        match self {
            InnerTableState::Ready(data) => Ok(data),
            _ => Err(Error::InvalidOperation {
                message: "Expected build side in ready state".to_string(),
                location: location!(),
            }),
        }
    }
}

#[derive(Debug)]
pub struct NestedLoopJoinExec {
    /// The left side `ExecutionPlan` of the join operation.
    lhs: Arc<dyn ExecutionPlan>,
    /// The right side `ExecutionPlan` of the join operation.
    rhs: Arc<dyn ExecutionPlan>,
    /// An optional `JoinFilter` applied to the join operation.
    filter: Option<JoinFilter>,
    /// The `JoinType` (e.g. `Inner`, `Left`).
    join_type: JoinType,
    /// The output schema, after the join operation.
    schema: SchemaRef,
    /// The columns involved in a join operation.
    column_indices: Vec<JoinColumnIndex>,
}

impl NestedLoopJoinExec {
    pub fn try_new(
        lhs: Arc<dyn ExecutionPlan>,
        rhs: Arc<dyn ExecutionPlan>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
    ) -> Result<Self> {
        let left_schema = lhs.schema();
        let right_schema = rhs.schema();

        is_valid_join(&left_schema, &right_schema, &vec![])?;
        let (schema, column_indices) = create_join_schema(&left_schema, &right_schema, &join_type);

        Ok(Self {
            lhs,
            rhs,
            filter,
            join_type,
            schema,
            column_indices,
        })
    }

    async fn collect_build_input(lhs: Arc<dyn ExecutionPlan>) -> Result<InnerTableData> {
        let schema = lhs.schema();
        let stream = lhs.execute()?;

        let init = (Vec::new(), 0);
        let (batches, num_rows) = stream
            .try_fold(init, |mut acc, batch| async {
                acc.1 += batch.num_rows();
                acc.0.push(batch);
                Ok(acc)
            })
            .await?;
        let batch = concat_batches(&schema, batches.iter())?;
        let mut visited = BooleanBufferBuilder::new(batch.num_rows());
        visited.append_n(num_rows, false);

        Ok(InnerTableData::new(batch, visited))
    }
}

impl ExecutionPlan for NestedLoopJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&dyn ExecutionPlan> {
        vec![self.lhs.as_ref(), self.rhs.as_ref()]
    }

    fn execute(&self) -> Result<RecordBatchStream> {
        let lhs = self.lhs.clone();
        let inner_table =
            InnerTableState::Initial(async move { Self::collect_build_input(lhs).await }.boxed());
        let outer_table = self.rhs.execute()?;
        let stream = NestedLoopJoinStream {
            schema: self.schema(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            inner_table,
            outer_table,
            column_indices: self.column_indices.clone(),
            is_exhaused: false,
        };

        Ok(stream.boxed())
    }

    fn format(&self) -> String {
        format!(
            "NestedLoopJoinExec: [type: {}, filter: {:?}]",
            self.join_type, self.filter
        )
    }
}

impl Display for NestedLoopJoinExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_exec(self, f, 0)
    }
}

struct NestedLoopJoinStream {
    schema: SchemaRef,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    inner_table: InnerTableState,
    outer_table: RecordBatchStream,
    column_indices: Vec<JoinColumnIndex>,
    is_exhaused: bool,
}

impl NestedLoopJoinStream {
    fn poll_next_inner(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        todo!()
    }
}

impl Stream for NestedLoopJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
    }
}

#[cfg(test)]
mod tests {}

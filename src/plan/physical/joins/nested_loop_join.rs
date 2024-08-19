use std::{
    any::Any,
    fmt::{Debug, Display},
    sync::Arc,
    task::Poll,
};

use arrow::{
    array::{BooleanBufferBuilder, UInt32Builder, UInt64Builder},
    compute::concat_batches,
};
use arrow_array::{RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::SchemaRef;
use futures::{future::BoxFuture, ready, FutureExt, Stream, StreamExt, TryStreamExt};
use snafu::location;

use crate::{
    error::{Error, Result},
    io::RecordBatchStream,
    plan::{
        logical::join::JoinType,
        physical::{
            joins::utils::{build_batch_from_indices, create_join_schema, is_valid_join, JoinSide},
            plan::{format_exec, ExecutionPlan},
        },
    },
};

use super::utils::{apply_join_filter, JoinColumnIndex, JoinFilter};

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
            outer_table_schema: self.rhs.schema(),
            column_indices: self.column_indices.clone(),
            state: NestedLoopJoinStreamState::WaitBuildSide,
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

enum StreamResultState<T> {
    Ready(T),
    Continue,
}

macro_rules! handle_stream_result {
    ($e: expr) => {
        match $e {
            Ok(StreamResultState::Continue) => continue,
            Ok(StreamResultState::Ready(res)) => Poll::Ready(Ok(res).transpose()),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    };
}

enum NestedLoopJoinStreamState {
    WaitBuildSide,
    FetchProbeBatch,
    ProcessProbeBatch(RecordBatch),
    ExhaustedProbeSide,
    Completed,
}

impl NestedLoopJoinStreamState {
    fn try_as_process_probe_batch(&self) -> Result<&RecordBatch> {
        match self {
            NestedLoopJoinStreamState::ProcessProbeBatch(batch) => Ok(batch),
            _ => Err(Error::InvalidOperation {
                message: "Expected stream in processing state".to_string(),
                location: location!(),
            }),
        }
    }
}

struct NestedLoopJoinStream {
    schema: SchemaRef,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    inner_table: InnerTableState,
    outer_table: RecordBatchStream,
    outer_table_schema: SchemaRef,
    column_indices: Vec<JoinColumnIndex>,
    state: NestedLoopJoinStreamState,
}

impl NestedLoopJoinStream {
    fn poll_next_inner(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        use NestedLoopJoinStreamState::*;

        loop {
            return match self.state {
                WaitBuildSide => {
                    handle_stream_result!(ready!(self.collect_build_side(cx)))
                }
                FetchProbeBatch => {
                    handle_stream_result!(ready!(self.fetch_probe_batch(cx)))
                }
                ProcessProbeBatch(_) => {
                    handle_stream_result!(self.process_probe_batch())
                }
                ExhaustedProbeSide => {
                    handle_stream_result!(self.process_unmatched_build_batch())
                }
                Completed => Poll::Ready(None),
            };
        }
    }

    fn collect_build_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StreamResultState<Option<RecordBatch>>>> {
        let inner_table = match self.inner_table.try_as_init_mut()?.poll_unpin(cx) {
            Poll::Ready(data) => data?,
            Poll::Pending => return Poll::Pending,
        };

        self.state = NestedLoopJoinStreamState::FetchProbeBatch;
        self.inner_table = InnerTableState::Ready(inner_table);

        Poll::Ready(Ok(StreamResultState::Continue))
    }

    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StreamResultState<Option<RecordBatch>>>> {
        match ready!(self.outer_table.poll_next_unpin(cx)) {
            None => self.state = NestedLoopJoinStreamState::ExhaustedProbeSide,
            Some(Ok(batch)) => self.state = NestedLoopJoinStreamState::ProcessProbeBatch(batch),
            Some(Err(e)) => return Poll::Ready(Err(e)),
        };
        Poll::Ready(Ok(StreamResultState::Continue))
    }

    fn process_probe_batch(&mut self) -> Result<StreamResultState<Option<RecordBatch>>> {
        let inner_table = self.inner_table.try_as_ready_mut()?;
        let build_batch = &inner_table.batch;
        let probe_batch = self.state.try_as_process_probe_batch()?;
        let visited = &mut inner_table.visited;

        let result = Self::join_build_probe_batch(
            build_batch,
            probe_batch,
            &self.join_type,
            &self.filter,
            &self.column_indices,
            self.schema.clone(),
            visited,
        )?;

        self.state = NestedLoopJoinStreamState::FetchProbeBatch;

        Ok(StreamResultState::Ready(Some(result)))
    }

    fn join_build_probe_batch(
        build_batch: &RecordBatch,
        probe_batch: &RecordBatch,
        join_type: &JoinType,
        filter: &Option<JoinFilter>,
        column_indices: &[JoinColumnIndex],
        schema: SchemaRef,
        visited: &mut BooleanBufferBuilder,
    ) -> Result<RecordBatch> {
        let join_indices = (0..build_batch.num_rows())
            .map(|build_idx| {
                let probe_row_count = probe_batch.num_rows();
                let build_indices = UInt64Array::from(vec![build_idx as u64; probe_row_count]);
                let probe_indices = UInt32Array::from_iter_values(0..(probe_row_count as u32));
                apply_join_filter(
                    filter,
                    build_indices,
                    probe_indices,
                    build_batch,
                    probe_batch,
                    JoinSide::Left,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let mut build_indices_builder = UInt64Builder::new();
        let mut probe_indices_builder = UInt32Builder::new();
        for (build_indices, probe_indices) in join_indices {
            build_indices_builder
                .append_values(build_indices.values(), &vec![true; build_indices.len()]);
            probe_indices_builder
                .append_values(probe_indices.values(), &vec![true; probe_indices.len()]);
        }
        let build_indices = build_indices_builder.finish();
        let probe_indices = probe_indices_builder.finish();

        // adjust indices according to join_type
        let (build_indices, probe_indices) = match join_type {
            JoinType::Inner | JoinType::Left => (build_indices, probe_indices),
        };

        build_indices
            .iter()
            .flatten()
            .for_each(|idx| visited.set_bit(idx as usize, true));

        build_batch_from_indices(
            schema,
            build_batch,
            probe_batch,
            &build_indices,
            &probe_indices,
            column_indices,
            JoinSide::Left,
        )
    }

    fn process_unmatched_build_batch(&mut self) -> Result<StreamResultState<Option<RecordBatch>>> {
        if !matches!(self.join_type, JoinType::Left) {
            self.state = NestedLoopJoinStreamState::Completed;
            return Ok(StreamResultState::Continue);
        }
        let build_side = self.inner_table.try_as_ready_mut()?;
        let build_batch = &build_side.batch;
        let build_size = build_side.visited.len();

        let build_indices_unmatched = (0..build_size)
            .filter_map(|idx| (!build_side.visited.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>();

        let mut builder = UInt32Builder::with_capacity(build_indices_unmatched.len());
        builder.append_nulls(build_indices_unmatched.len());
        let probe_indices = builder.finish();
        let probe_batch = RecordBatch::new_empty(self.outer_table_schema.clone());

        let result = build_batch_from_indices(
            self.schema.clone(),
            build_batch,
            &probe_batch,
            &build_indices_unmatched,
            &probe_indices,
            &self.column_indices,
            JoinSide::Left,
        )?;

        self.state = NestedLoopJoinStreamState::Completed;

        Ok(StreamResultState::Ready(Some(result)))
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
mod tests {
    use std::sync::Arc;

    use arrow::compute::concat_batches;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

    use crate::{
        expression::{
            operator::Operator,
            physical::{binary::BinaryExpr, column::ColumnExpr, literal::LiteralExpr},
            values::ScalarValue,
        },
        io::reader::csv::options::CsvFileOpenerConfig,
        plan::{
            logical::join::JoinType,
            physical::{
                joins::utils::{JoinColumnIndex, JoinFilter, JoinSide},
                plan::ExecutionPlan,
                scan::csv::CsvExec,
            },
        },
    };

    use super::NestedLoopJoinExec;

    fn create_nested_loop_join(
        join_type: JoinType,
        filter: Option<JoinFilter>,
    ) -> NestedLoopJoinExec {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("l1", DataType::Utf8, true),
            Field::new("l2", DataType::Int64, true),
            Field::new("l3", DataType::Int64, true),
        ]));
        let lhs = CsvExec::new(
            "testdata/csv/join_left.csv",
            CsvFileOpenerConfig::new(left_schema),
        );
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("r1", DataType::Utf8, true),
            Field::new("r2", DataType::Int64, true),
            Field::new("r3", DataType::Int64, true),
        ]));
        let rhs = CsvExec::new(
            "testdata/csv/join_right.csv",
            CsvFileOpenerConfig::new(right_schema),
        );
        NestedLoopJoinExec::try_new(Arc::new(lhs), Arc::new(rhs), filter, join_type).unwrap()
    }

    #[tokio::test]
    async fn test_nested_loop_join_left_with_filters() {
        let intermediate_schema =
            Arc::new(Schema::new(vec![Field::new("l2", DataType::Int64, true)]));
        let column_indices = vec![JoinColumnIndex::new(1, JoinSide::Left)];
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("l2", 0)),
            Operator::NotEq,
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(1)))),
        ));
        let filter = JoinFilter::new(intermediate_schema, expression, column_indices);
        let exec = create_nested_loop_join(JoinType::Left, Some(filter));
        let schema = exec.schema();

        let mut stream = exec.execute().unwrap();

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }

        let final_batch = concat_batches(&schema, batches.iter()).unwrap();
        assert_eq!(final_batch.num_rows(), 16);
        assert_eq!(final_batch.num_columns(), 6);
    }

    #[tokio::test]
    async fn test_nested_loop_join_left_no_filters() {
        let exec = create_nested_loop_join(JoinType::Left, None);
        let schema = exec.schema();

        let mut stream = exec.execute().unwrap();

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }

        let final_batch = concat_batches(&schema, batches.iter()).unwrap();
        assert_eq!(final_batch.num_rows(), 18);
        assert_eq!(final_batch.num_columns(), 6);
    }

    #[tokio::test]
    async fn test_nested_loop_join_inner_with_filters() {
        let intermediate_schema =
            Arc::new(Schema::new(vec![Field::new("l2", DataType::Int64, true)]));
        let column_indices = vec![JoinColumnIndex::new(1, JoinSide::Right)];
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("r2", 0)),
            Operator::NotEq,
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(100)))),
        ));
        let filter = JoinFilter::new(intermediate_schema, expression, column_indices);
        let exec = create_nested_loop_join(JoinType::Inner, Some(filter));
        let schema = exec.schema();

        let mut stream = exec.execute().unwrap();

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }

        let final_batch = concat_batches(&schema, batches.iter()).unwrap();
        assert_eq!(final_batch.num_rows(), 12);
        assert_eq!(final_batch.num_columns(), 6);
    }

    #[tokio::test]
    async fn test_nested_loop_join_inner_no_filters() {
        let exec = create_nested_loop_join(JoinType::Inner, None);
        let schema = exec.schema();

        let mut stream = exec.execute().unwrap();

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }

        let final_batch = concat_batches(&schema, batches.iter()).unwrap();
        assert_eq!(final_batch.num_rows(), 18);
        assert_eq!(final_batch.num_columns(), 6);
    }
}

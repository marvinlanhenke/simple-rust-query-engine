use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    sync::Arc,
    task::{ready, Poll},
};

use ahash::RandomState;
use arrow::{
    array::{
        as_boolean_array, downcast_array, BooleanBufferBuilder, UInt32BufferBuilder, UInt32Builder,
        UInt64BufferBuilder,
    },
    compute::{self, concat_batches},
};
use arrow_array::{
    new_null_array, Array, ArrayRef, PrimitiveArray, RecordBatch, RecordBatchOptions, UInt32Array,
    UInt64Array,
};
use arrow_schema::{Schema, SchemaBuilder, SchemaRef};
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt, TryStreamExt};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::physical::{column::ColumnExpr, expr::PhysicalExpression},
    io::RecordBatchStream,
    plan::{
        logical::join::JoinType,
        physical::plan::{format_exec, ExecutionPlan},
    },
};

use super::utils::create_hashes;

const DEFAULT_BATCH_SIZE: usize = 1024;

pub type JoinOn = Vec<(Arc<dyn PhysicalExpression>, Arc<dyn PhysicalExpression>)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy)]
pub struct JoinColumnIndex {
    index: usize,
    side: JoinSide,
}

#[derive(Debug, Clone)]
pub struct JoinFilter {
    schema: SchemaRef,
    expression: Arc<dyn PhysicalExpression>,
    column_indices: Vec<JoinColumnIndex>,
}

impl JoinFilter {
    pub fn new(
        schema: SchemaRef,
        expression: Arc<dyn PhysicalExpression>,
        column_indices: Vec<JoinColumnIndex>,
    ) -> JoinFilter {
        JoinFilter {
            expression,
            column_indices,
            schema,
        }
    }
}

#[derive(Debug)]
struct JoinLeftData {
    map: HashMap<u64, u64>,
    batch: RecordBatch,
    /// Bitmap builder for visited left indices.
    visited: BooleanBufferBuilder,
}

#[derive(Debug)]
pub struct HashJoinExec {
    lhs: Arc<dyn ExecutionPlan>,
    rhs: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    /// The output schema, after the join operation.
    schema: SchemaRef,
    column_indices: Vec<JoinColumnIndex>,
    random_state: RandomState,
}

impl HashJoinExec {
    pub fn try_new(
        lhs: Arc<dyn ExecutionPlan>,
        rhs: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: JoinType,
    ) -> Result<Self> {
        if on.is_empty() {
            return Err(Error::InvalidData {
                message: "The 'JoinOn' constraints should not be empty".to_string(),
                location: location!(),
            });
        }

        let left_schema = lhs.schema();
        let right_schema = rhs.schema();

        Self::is_valid_join(&left_schema, &right_schema, &on)?;

        let (schema, column_indices) =
            Self::create_join_schema(&left_schema, &right_schema, &join_type);

        Ok(Self {
            lhs,
            rhs,
            on,
            filter,
            join_type,
            schema,
            column_indices,
            random_state: RandomState::new(),
        })
    }

    /// Checks if the columns intersection from both schemas matches the `JoinOn` condition.
    fn is_valid_join(left_schema: &Schema, right_schema: &Schema, on: &JoinOn) -> Result<()> {
        let extract_columns = |schema: &Schema| -> HashSet<ColumnExpr> {
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| ColumnExpr::new(field.name(), idx))
                .collect()
        };

        let extract_join_columns = |on: &JoinOn, side: JoinSide| -> Result<HashSet<ColumnExpr>> {
            on.iter()
                .map(|expr| {
                    let expr = match side {
                        JoinSide::Left => &expr.0,
                        JoinSide::Right => &expr.1,
                    };
                    expr.as_any()
                        .downcast_ref::<ColumnExpr>()
                        .ok_or_else(|| Error::InvalidOperation {
                            message: "failed to downcast expression".to_string(),
                            location: location!(),
                        })
                        .cloned()
                })
                .collect()
        };

        let left = extract_columns(left_schema);
        let left_on = extract_join_columns(on, JoinSide::Left)?;
        let left_missing = left_on.difference(&left).collect::<HashSet<_>>();

        let right = extract_columns(right_schema);
        let right_on = extract_join_columns(on, JoinSide::Right)?;
        let right_missing = right_on.difference(&right).collect::<HashSet<_>>();

        if !left_missing.is_empty() | !right_missing.is_empty() {
            return Err(Error::InvalidData { message: "one side of the join does not have all columns that are required by the 'on' join condition".to_string(), location: location!() });
        }

        Ok(())
    }

    /// Creates a schema for a join operation, starting with the left sides fields.
    fn create_join_schema(
        left_schema: &Schema,
        right_schema: &Schema,
        join_type: &JoinType,
    ) -> (SchemaRef, Vec<JoinColumnIndex>) {
        use JoinType::*;

        let (fields, column_indices): (SchemaBuilder, Vec<JoinColumnIndex>) = match join_type {
            Inner | Left => {
                let left_fields = left_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        (
                            field.clone(),
                            JoinColumnIndex {
                                index,
                                side: JoinSide::Left,
                            },
                        )
                    });
                let right_fields =
                    right_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(index, field)| {
                            (
                                field.clone(),
                                JoinColumnIndex {
                                    index,
                                    side: JoinSide::Right,
                                },
                            )
                        });
                left_fields.chain(right_fields).unzip()
            }
        };

        (Arc::new(fields.finish()), column_indices)
    }

    async fn collect_build_input(
        lhs: Arc<dyn ExecutionPlan>,
        lhs_on: Vec<Arc<dyn PhysicalExpression>>,
        random_state: RandomState,
    ) -> Result<JoinLeftData> {
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

        let mut map: HashMap<u64, u64> = HashMap::with_capacity(num_rows);
        let mut hashes_buffer: Vec<u64> = Vec::new();

        let input_batches = batches.iter().rev();
        for batch in input_batches.clone() {
            hashes_buffer.clear();
            hashes_buffer.resize(batch.num_rows(), 0);
            Self::update_hash(&lhs_on, batch, &mut map, &mut hashes_buffer, &random_state)?;
        }
        let batch = concat_batches(&schema, input_batches)?;
        let mut visited = BooleanBufferBuilder::new(batch.num_rows());
        visited.append_n(num_rows, false);

        Ok(JoinLeftData {
            map,
            batch,
            visited,
        })
    }

    /// Updates `JoinHashMap` by evaluating and hashing the `join-on` key expressions.
    fn update_hash(
        on: &[Arc<dyn PhysicalExpression>],
        batch: &RecordBatch,
        map: &mut HashMap<u64, u64>,
        hashes_buffer: &mut Vec<u64>,
        random_state: &RandomState,
    ) -> Result<()> {
        let keys = on
            .iter()
            .map(|expr| expr.eval(batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;

        let hash_values = create_hashes(&keys, hashes_buffer, random_state)?;

        // TODO: Handle hash_collisions, otherwise we cannot track
        // rows that have the same hashvalue but map to different row indices.
        for (row_idx, hash) in hash_values.iter().enumerate() {
            map.insert(*hash, row_idx as u64);
        }

        Ok(())
    }
}

impl ExecutionPlan for HashJoinExec {
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
        let on_left = self
            .on
            .iter()
            .map(|expr| expr.0.clone())
            .collect::<Vec<_>>();
        let on_right = self
            .on
            .iter()
            .map(|expr| expr.1.clone())
            .collect::<Vec<_>>();

        let probe_input = self.rhs.execute()?;

        let lhs = self.lhs.clone();
        let lhs_on = on_left.clone();
        let random_state = self.random_state.clone();
        let build_input_future =
            async move { Self::collect_build_input(lhs, lhs_on, random_state).await }.boxed();

        let stream = HashJoinStream {
            schema: self.schema(),
            on_right,
            filter: self.filter.clone(),
            join_type: self.join_type,
            build_side: BuildSideState::Initial(build_input_future),
            probe_input,
            probe_schema: self.rhs.schema(),
            column_indices: self.column_indices.clone(),
            state: HashJoinStreamState::WaitBuildSide,
            batch_size: DEFAULT_BATCH_SIZE,
            hashes_buffer: vec![],
            random_state: self.random_state.clone(),
        };

        Ok(stream.boxed())
    }

    fn format(&self) -> String {
        format!(
            "HashJoinExec: [type: {}, on: {:?}, filter: {:?}]",
            self.join_type, self.on, self.filter
        )
    }
}

impl Display for HashJoinExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_exec(self, f, 0)
    }
}

type BuildInputFuture = BoxFuture<'static, Result<JoinLeftData>>;

enum BuildSideState {
    Initial(BuildInputFuture),
    Ready(JoinLeftData),
}

impl BuildSideState {
    fn try_as_init_mut(&mut self) -> Result<&mut BuildInputFuture> {
        match self {
            BuildSideState::Initial(fut) => Ok(fut),
            _ => Err(Error::InvalidOperation {
                message: "Expected build side in initial state".to_string(),
                location: location!(),
            }),
        }
    }

    fn try_as_ready_mut(&mut self) -> Result<&mut JoinLeftData> {
        match self {
            BuildSideState::Ready(join_left_data) => Ok(join_left_data),
            _ => Err(Error::InvalidOperation {
                message: "Expected build side in ready state".to_string(),
                location: location!(),
            }),
        }
    }
}

/// Represents the stream result.
/// Indicated whether the stream produced a result that is ready for use
/// or if the operation is required to continue.
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

enum HashJoinStreamState {
    WaitBuildSide,
    FetchProbeBatch,
    ProcessProbeBatch(RecordBatch),
    ExhaustedProbeSide,
    Completed,
}

impl HashJoinStreamState {
    fn try_as_process_probe_batch(&self) -> Result<&RecordBatch> {
        match self {
            HashJoinStreamState::ProcessProbeBatch(batch) => Ok(batch),
            _ => Err(Error::InvalidOperation {
                message: "Expected stream in processing state".to_string(),
                location: location!(),
            }),
        }
    }
}

struct HashJoinStream {
    /// The input schema.
    schema: SchemaRef,
    on_right: Vec<Arc<dyn PhysicalExpression>>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    build_side: BuildSideState,
    /// The right (probe) input stream.
    probe_input: RecordBatchStream,
    probe_schema: SchemaRef,
    column_indices: Vec<JoinColumnIndex>,
    state: HashJoinStreamState,
    /// Maximum output batch size.
    batch_size: usize,
    /// Internal buffer for computing hashes.
    hashes_buffer: Vec<u64>,
    random_state: RandomState,
}

impl HashJoinStream {
    fn poll_next_inner(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        use HashJoinStreamState::*;

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
        let left_data = match self.build_side.try_as_init_mut()?.poll_unpin(cx) {
            Poll::Ready(left_data) => left_data?,
            Poll::Pending => return Poll::Pending,
        };

        self.state = HashJoinStreamState::FetchProbeBatch;
        self.build_side = BuildSideState::Ready(left_data);

        Poll::Ready(Ok(StreamResultState::Continue))
    }

    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StreamResultState<Option<RecordBatch>>>> {
        match ready!(self.probe_input.poll_next_unpin(cx)) {
            None => self.state = HashJoinStreamState::ExhaustedProbeSide,
            Some(Ok(batch)) => {
                let keys = self
                    .on_right
                    .iter()
                    .map(|expr| expr.eval(&batch)?.into_array(batch.num_rows()))
                    .collect::<Result<Vec<_>>>()?;
                self.hashes_buffer.clear();
                self.hashes_buffer.resize(batch.num_rows(), 0);
                create_hashes(&keys, &mut self.hashes_buffer, &self.random_state)?;
                self.state = HashJoinStreamState::ProcessProbeBatch(batch);
            }
            Some(Err(e)) => return Poll::Ready(Err(e)),
        };
        Poll::Ready(Ok(StreamResultState::Continue))
    }

    fn process_probe_batch(&mut self) -> Result<StreamResultState<Option<RecordBatch>>> {
        let build_side = self.build_side.try_as_ready_mut()?;
        let map = &build_side.map;
        let build_batch = &build_side.batch;
        let probe_batch = self.state.try_as_process_probe_batch()?;
        let visited = &mut build_side.visited;

        // get matched join-key indices, check for equal rows
        // apply join filters if exists
        // build output batch from indices
        let (build_indices, probe_indices) =
            Self::get_matched_join_key_indices(map, &self.hashes_buffer, self.batch_size)?;
        let (build_indices, probe_indices) = Self::apply_join_filter(
            &self.filter,
            build_indices,
            probe_indices,
            build_batch,
            probe_batch,
            JoinSide::Left,
        )?;
        if matches!(self.join_type, JoinType::Left) {
            build_indices.iter().flatten().for_each(|x| {
                visited.set_bit(x as usize, true);
            });
        }
        let result = Self::build_batch_from_indices(
            self.schema.clone(),
            build_batch,
            probe_batch,
            &build_indices,
            &probe_indices,
            &self.column_indices,
            JoinSide::Left,
        )?;

        self.state = HashJoinStreamState::FetchProbeBatch;

        Ok(StreamResultState::Ready(Some(result)))
    }

    fn get_matched_join_key_indices(
        map: &HashMap<u64, u64>,
        hashes_buffer: &[u64],
        batch_size: usize,
    ) -> Result<(UInt64Array, UInt32Array)> {
        let mut build_indices_builder = UInt64BufferBuilder::new(0);
        let mut probe_indices_builder = UInt32BufferBuilder::new(0);
        let mut processed_rows = 0;
        for (probe_idx, hash_value) in hashes_buffer.iter().enumerate() {
            if processed_rows >= batch_size {
                break;
            }
            if let Some(build_idx) = map.get(hash_value) {
                build_indices_builder.append(*build_idx);
                probe_indices_builder.append(probe_idx as u32);
                processed_rows += 1;
            }
        }
        let build_indices: UInt64Array =
            PrimitiveArray::new(build_indices_builder.finish().into(), None);
        let probe_indices: UInt32Array =
            PrimitiveArray::new(probe_indices_builder.finish().into(), None);
        Ok((build_indices, probe_indices))
    }

    fn apply_join_filter(
        filter: &Option<JoinFilter>,
        build_indices: UInt64Array,
        probe_indices: UInt32Array,
        build_batch: &RecordBatch,
        probe_batch: &RecordBatch,
        side: JoinSide,
    ) -> Result<(UInt64Array, UInt32Array)> {
        if build_indices.is_empty() && probe_indices.is_empty() {
            return Ok((build_indices, probe_indices));
        }

        if let Some(filter) = filter {
            // build intermediate batch
            // evaluate filter expr -> boolean mask
            // apply filter mask to build and probe indices
            let intermediate_batch = Self::build_batch_from_indices(
                filter.schema.clone(),
                build_batch,
                probe_batch,
                &build_indices,
                &probe_indices,
                filter.column_indices.as_slice(),
                side,
            )?;
            let filter_result = filter
                .expression
                .eval(&intermediate_batch)?
                .into_array(intermediate_batch.num_rows())?;
            let filter_mask = as_boolean_array(&filter_result);
            let build_filtered = compute::filter(&build_indices, filter_mask)?;
            let probe_filtered = compute::filter(&probe_indices, filter_mask)?;

            Ok((
                downcast_array(build_filtered.as_ref()),
                downcast_array(probe_filtered.as_ref()),
            ))
        } else {
            Ok((build_indices, probe_indices))
        }
    }

    fn build_batch_from_indices(
        schema: SchemaRef,
        build_batch: &RecordBatch,
        probe_batch: &RecordBatch,
        build_indices: &UInt64Array,
        probe_indices: &UInt32Array,
        column_indices: &[JoinColumnIndex],
        side: JoinSide,
    ) -> Result<RecordBatch> {
        if schema.fields().is_empty() {
            let options = RecordBatchOptions::new()
                .with_match_field_names(true)
                .with_row_count(Some(build_indices.len()));
            return Ok(RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &options,
            )?);
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for col_idx in column_indices {
            let array = match col_idx.side == side {
                true => {
                    let array = build_batch.column(col_idx.index);
                    if array.is_empty() || build_indices.null_count() == build_indices.len() {
                        new_null_array(array.data_type(), build_indices.len())
                    } else {
                        compute::take(array.as_ref(), build_indices, None)?
                    }
                }
                false => {
                    let array = probe_batch.column(col_idx.index);
                    if array.is_empty() || probe_indices.null_count() == probe_indices.len() {
                        new_null_array(array.data_type(), probe_indices.len())
                    } else {
                        compute::take(array.as_ref(), probe_indices, None)?
                    }
                }
            };
            columns.push(array);
        }

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn process_unmatched_build_batch(&mut self) -> Result<StreamResultState<Option<RecordBatch>>> {
        if !matches!(self.join_type, JoinType::Left) {
            self.state = HashJoinStreamState::Completed;
            return Ok(StreamResultState::Continue);
        }

        let build_side = self.build_side.try_as_ready_mut()?;
        let build_batch = &build_side.batch;
        let build_size = build_side.visited.len();

        let build_indices_unmatched = (0..build_size)
            .filter_map(|idx| (!build_side.visited.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>();

        let mut builder = UInt32Builder::with_capacity(build_indices_unmatched.len());
        builder.append_nulls(build_indices_unmatched.len());
        let probe_indices = builder.finish();
        let probe_batch = RecordBatch::new_empty(self.probe_schema.clone());

        let result = Self::build_batch_from_indices(
            self.schema.clone(),
            build_batch,
            &probe_batch,
            &build_indices_unmatched,
            &probe_indices,
            &self.column_indices,
            JoinSide::Left,
        )?;

        self.state = HashJoinStreamState::Completed;

        Ok(StreamResultState::Ready(Some(result)))
    }
}

impl Stream for HashJoinStream {
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
                joins::hash_join::{JoinColumnIndex, JoinSide},
                plan::ExecutionPlan,
                scan::csv::CsvExec,
            },
        },
        tests::create_schema,
    };

    use super::{HashJoinExec, JoinFilter, JoinOn};

    fn create_hash_join(
        on: JoinOn,
        join_type: JoinType,
        filter: Option<JoinFilter>,
    ) -> HashJoinExec {
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

        HashJoinExec::try_new(Arc::new(lhs), Arc::new(rhs), on, filter, join_type).unwrap()
    }
    #[tokio::test]
    async fn test_inner_join_with_filter() {
        let on: JoinOn = vec![(
            Arc::new(ColumnExpr::new("l1", 0)),
            Arc::new(ColumnExpr::new("r1", 0)),
        )];
        let intermediate_schema =
            Arc::new(Schema::new(vec![Field::new("r2", DataType::Int64, true)]));
        let column_indices = vec![JoinColumnIndex {
            index: 1,
            side: JoinSide::Right,
        }];
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("r2", 0)),
            Operator::NotEq,
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(100)))),
        ));
        let filter = JoinFilter::new(intermediate_schema, expression, column_indices);
        let exec = create_hash_join(on, JoinType::Inner, Some(filter));
        let schema = exec.schema();

        let mut stream = exec.execute().unwrap();

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }

        let final_batch = concat_batches(&schema, batches.iter()).unwrap();
        assert_eq!(final_batch.num_rows(), 2);
        assert_eq!(final_batch.num_columns(), 6);
    }

    #[tokio::test]
    async fn test_inner_join_no_filter() {
        let on: JoinOn = vec![(
            Arc::new(ColumnExpr::new("l1", 0)),
            Arc::new(ColumnExpr::new("r1", 0)),
        )];
        let exec = create_hash_join(on, JoinType::Inner, None);
        let schema = exec.schema();

        let mut stream = exec.execute().unwrap();

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }

        let final_batch = concat_batches(&schema, batches.iter()).unwrap();
        assert_eq!(final_batch.num_rows(), 3);
        assert_eq!(final_batch.num_columns(), 6);
    }

    #[tokio::test]
    async fn test_left_join_with_filter() {
        let on: JoinOn = vec![(
            Arc::new(ColumnExpr::new("l1", 0)),
            Arc::new(ColumnExpr::new("r1", 0)),
        )];
        let intermediate_schema =
            Arc::new(Schema::new(vec![Field::new("l2", DataType::Int64, true)]));
        let column_indices = vec![JoinColumnIndex {
            index: 1,
            side: JoinSide::Left,
        }];
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("l2", 0)),
            Operator::NotEq,
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(1)))),
        ));
        let filter = JoinFilter::new(intermediate_schema, expression, column_indices);
        let exec = create_hash_join(on, JoinType::Left, Some(filter));
        let schema = exec.schema();

        let mut stream = exec.execute().unwrap();

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = stream.next().await {
            println!("{batch:?}");
            batches.push(batch);
        }

        let final_batch = concat_batches(&schema, batches.iter()).unwrap();
        assert_eq!(final_batch.num_rows(), 6);
        assert_eq!(final_batch.num_columns(), 6);
    }

    #[tokio::test]
    async fn test_left_join_no_filter() {
        let on: JoinOn = vec![(
            Arc::new(ColumnExpr::new("l1", 0)),
            Arc::new(ColumnExpr::new("r1", 0)),
        )];
        let exec = create_hash_join(on, JoinType::Left, None);
        let schema = exec.schema();

        let mut stream = exec.execute().unwrap();

        let mut batches = Vec::new();
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }

        let final_batch = concat_batches(&schema, batches.iter()).unwrap();
        assert_eq!(final_batch.num_rows(), 6);
        assert_eq!(final_batch.num_columns(), 6);
    }

    #[test]
    fn test_create_join_schema() {
        let left_schema = create_schema();
        let right_schema = create_schema();

        let (schema, column_indices) =
            HashJoinExec::create_join_schema(&left_schema, &right_schema, &JoinType::Inner);
        assert_eq!(schema.fields().len(), 6);
        assert_eq!(column_indices.len(), 6);
    }

    #[test]
    fn test_is_valid_join() {
        let left_schema = create_schema();
        let right_schema = create_schema();

        let result = HashJoinExec::is_valid_join(
            &left_schema,
            &right_schema,
            &vec![(
                Arc::new(ColumnExpr::new("c1", 0)),
                Arc::new(ColumnExpr::new("c1", 0)),
            )],
        );
        assert!(result.is_ok());

        let result = HashJoinExec::is_valid_join(
            &left_schema,
            &right_schema,
            &vec![
                (
                    Arc::new(ColumnExpr::new("c1", 0)),
                    Arc::new(ColumnExpr::new("c1", 0)),
                ),
                (
                    Arc::new(ColumnExpr::new("c2", 1)),
                    Arc::new(ColumnExpr::new("c2", 1)),
                ),
            ],
        );
        assert!(result.is_ok());

        let result = HashJoinExec::is_valid_join(
            &left_schema,
            &right_schema,
            &vec![(
                Arc::new(ColumnExpr::new("c1", 0)),
                Arc::new(ColumnExpr::new("c4", 3)),
            )],
        );
        assert!(result.is_err());
    }
}

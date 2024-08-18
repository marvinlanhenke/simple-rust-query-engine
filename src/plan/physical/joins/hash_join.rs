use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    sync::Arc,
    task::{ready, Poll},
};

use arrow_select::take::take;

use ahash::RandomState;
use arrow::{
    array::{
        as_boolean_array, downcast_array, BooleanBufferBuilder, UInt32BufferBuilder, UInt32Builder,
        UInt64BufferBuilder,
    },
    compute::{self, and, concat_batches, kernels::cmp::eq, FilterBuilder},
};
use arrow_array::{
    new_null_array, Array, ArrayRef, BooleanArray, PrimitiveArray, RecordBatch, RecordBatchOptions,
    UInt32Array, UInt64Array,
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
    utils::create_hashes,
};

use super::utils::{JoinColumnIndex, JoinFilter, JoinOn, JoinSide};

/// Default batch size for processing operations in a join operation.
/// Determines the number of rows processed in each batch, emitted by [`HashJoinExec`].
const DEFAULT_BATCH_SIZE: usize = 1024;

/// A type alias representing an offset within a [`JoinHashMap`].
///
/// The tuple contains:
/// - `usize`: The index from which to continue the join operation.
/// - `Option<u64>`: The next chain index, or `None` if no further chaining is needed.
type JoinHashMapOffset = (usize, Option<u64>);

/// A hash map used in join operations, mapping `u64` hash values to lists of row indices.
///
/// This struct handles hash collisions by storing row indices in a separate chained list (`Vec<u64>`).
/// When multiple rows map to the same hash value, the most recent row index is stored in the hash map,
/// while previous indices are linked via the `next` vector. The chain ends when the value `0` is reached,
/// indicating the end of the list.
///
/// Note that hash collisions do not guarantee value equality;
/// such checks must be performed separately outside of `JoinHashMap`.
///
/// # Example
///
/// Insert (10, 1)         Insert (20, 2)         Insert (10, 3) <-- collision!
///
/// map:                   map:                   map:
/// +--------+             ----------             ----------
/// | 10 | 2 |             | 10 | 2 |             | 10 | 4 |
/// +--------+             | 20 | 3 |             | 20 | 3 |
///                        ----------             ----------
///
/// next:                  next:                  next:
/// +-----------+          +-----------+          +-----------+
/// | 0 | 0 | 0 |          | 0 | 0 | 0 |          | 0 | 0 | 2 |  <-- hash value 10 maps to 4,2 (indices 3,1)
/// +-----------+          +-----------+          +-----------+  
///
#[derive(Debug)]
struct JoinHashMap {
    map: HashMap<u64, u64>,
    next: Vec<u64>,
}

impl JoinHashMap {
    /// Creates a new [`JoinHashMap`] with a specified capacity.
    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            next: vec![0; capacity + 1],
        }
    }

    /// Updates the `JoinHashMap` with values from an iterator.
    ///
    /// This method processes each `(row index, hash value)` pair, updating the hash map
    /// and chaining indices to ensure that duplicate hash values are properly linked.
    fn update_from_iter<'a>(&mut self, iter: impl Iterator<Item = (usize, &'a u64)>) {
        for (new_row_index, hash_value) in iter {
            if let Some(curr_row_index) = self.map.get_mut(hash_value) {
                // Entry already exists in the map.
                // Add the current index to the next chain and update the map with the new index.
                let prev_row_index = *curr_row_index;
                *curr_row_index = (new_row_index + 1) as u64;
                self.next[new_row_index] = prev_row_index;
            } else {
                self.map.insert(*hash_value, (new_row_index + 1) as u64);
            }
        }
    }

    /// Retrieves the matched indices based on hash values, with support for offsets and limits.
    ///
    /// This method finds all rows that match the provided hash values, respecting any specified
    /// offsets and limits on the number of results. It returns the matched row indices,
    /// the corresponding input indices, and an optional offset for further processing.
    fn get_matched_indices(
        &self,
        hash_values: &[u64],
        offset: JoinHashMapOffset,
        limit: usize,
    ) -> (
        UInt32BufferBuilder,
        UInt64BufferBuilder,
        Option<JoinHashMapOffset>,
    ) {
        let mut probe_indices = UInt32BufferBuilder::new(0);
        let mut build_indices = UInt64BufferBuilder::new(0);
        let mut remaining_output = limit;
        let hash_map = &self.map;
        let next_chain = &self.next;

        let to_skip = match offset {
            // `initial_index` processing has not started yet.
            (initial_index, None) => initial_index,
            // `initial_index` has already been processed and should be skipped.
            (initial_index, Some(0)) => initial_index + 1,
            // otherwise process remaining `initial_index` from previous iteration.
            (initial_index, Some(initial_next_index)) => {
                let next_offset = Self::chain_traverse(
                    &mut probe_indices,
                    &mut build_indices,
                    hash_values,
                    next_chain,
                    initial_index,
                    initial_next_index as usize,
                    &mut remaining_output,
                );

                if next_offset.is_some() {
                    return (probe_indices, build_indices, next_offset);
                }

                initial_index + 1
            }
        };

        let mut row_index = to_skip;
        for hash_value in &hash_values[to_skip..] {
            if let Some(index) = hash_map.get(hash_value) {
                let next_offset = Self::chain_traverse(
                    &mut probe_indices,
                    &mut build_indices,
                    hash_values,
                    next_chain,
                    row_index,
                    *index as usize,
                    &mut remaining_output,
                );

                if next_offset.is_some() {
                    return (probe_indices, build_indices, next_offset);
                }
            }
            row_index += 1;
        }

        (probe_indices, build_indices, None)
    }

    /// Traverses a chain of matched indices, updating the result buffers and checking limits.
    ///
    /// This method follows the chain of indices in the `next_chain` vector, appending matched
    /// row indices and input indices to the respective buffers. It respects the `remaining_output`
    /// limit and returns an optional offset for further processing if the limit is reached.
    fn chain_traverse(
        probe_indices: &mut UInt32BufferBuilder,
        build_indices: &mut UInt64BufferBuilder,
        hash_values: &[u64],
        next_chain: &[u64],
        initial_index: usize,
        initial_next_index: usize,
        remaining_output: &mut usize,
    ) -> Option<JoinHashMapOffset> {
        let mut build_row_index = initial_next_index - 1;

        loop {
            build_indices.append(build_row_index as u64);
            probe_indices.append(initial_index as u32);
            *remaining_output -= 1;

            let next = next_chain[build_row_index];

            if *remaining_output == 0 {
                // If this is the last output and no more chain values are left.
                let next_offset = if initial_index == hash_values.len() - 1 && next == 0 {
                    None
                } else {
                    Some((initial_index, Some(next)))
                };
                return next_offset;
            }

            if next == 0 {
                break;
            }
            build_row_index = next as usize - 1;
        }

        None
    }
}

type BuildSideFuture = BoxFuture<'static, Result<BuildSideData>>;

#[derive(Debug)]
struct BuildSideData {
    map: JoinHashMap,
    batch: RecordBatch,
    /// Bitmap builder for visited left indices.
    visited: BooleanBufferBuilder,
}

enum BuildSideState {
    Initial(BuildSideFuture),
    Ready(BuildSideData),
}

impl BuildSideState {
    fn try_as_init_mut(&mut self) -> Result<&mut BuildSideFuture> {
        match self {
            BuildSideState::Initial(fut) => Ok(fut),
            _ => Err(Error::InvalidOperation {
                message: "Expected build side in initial state".to_string(),
                location: location!(),
            }),
        }
    }

    fn try_as_ready_mut(&mut self) -> Result<&mut BuildSideData> {
        match self {
            BuildSideState::Ready(join_left_data) => Ok(join_left_data),
            _ => Err(Error::InvalidOperation {
                message: "Expected build side in ready state".to_string(),
                location: location!(),
            }),
        }
    }
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
                        (field.clone(), JoinColumnIndex::new(index, JoinSide::Left))
                    });
                let right_fields =
                    right_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(index, field)| {
                            (field.clone(), JoinColumnIndex::new(index, JoinSide::Right))
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
    ) -> Result<BuildSideData> {
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

        let mut map = JoinHashMap::with_capacity(num_rows);
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

        Ok(BuildSideData {
            map,
            batch,
            visited,
        })
    }

    /// Updates `JoinHashMap` by evaluating and hashing the `join-on` key expressions.
    fn update_hash(
        on: &[Arc<dyn PhysicalExpression>],
        batch: &RecordBatch,
        map: &mut JoinHashMap,
        hashes_buffer: &mut Vec<u64>,
        random_state: &RandomState,
    ) -> Result<()> {
        let keys = on
            .iter()
            .map(|expr| expr.eval(batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;

        let hash_values = create_hashes(&keys, hashes_buffer, random_state)?;
        let iter = hash_values.iter().enumerate();
        map.update_from_iter(iter);

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
        let build_on = self
            .on
            .iter()
            .map(|expr| expr.0.clone())
            .collect::<Vec<_>>();
        let probe_on = self
            .on
            .iter()
            .map(|expr| expr.1.clone())
            .collect::<Vec<_>>();

        let probe_input = self.rhs.execute()?;

        let lhs = self.lhs.clone();
        let lhs_on = build_on.clone();
        let random_state = self.random_state.clone();
        let build_side_future =
            async move { Self::collect_build_input(lhs, lhs_on, random_state).await }.boxed();

        let stream = HashJoinStream {
            schema: self.schema(),
            build_on,
            probe_on,
            filter: self.filter.clone(),
            join_type: self.join_type,
            build_side: BuildSideState::Initial(build_side_future),
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

struct ProcessProbeBatchState {
    batch: RecordBatch,
    offset: JoinHashMapOffset,
    joined_probe_index: Option<usize>,
}

impl ProcessProbeBatchState {
    fn advance(&mut self, offset: JoinHashMapOffset, joined_probe_index: Option<usize>) {
        self.offset = offset;
        if joined_probe_index.is_some() {
            self.joined_probe_index = joined_probe_index;
        }
    }
}

enum HashJoinStreamState {
    WaitBuildSide,
    FetchProbeBatch,
    ProcessProbeBatch(ProcessProbeBatchState),
    ExhaustedProbeSide,
    Completed,
}

impl HashJoinStreamState {
    fn try_as_process_probe_batch(&mut self) -> Result<&mut ProcessProbeBatchState> {
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
    build_on: Vec<Arc<dyn PhysicalExpression>>,
    probe_on: Vec<Arc<dyn PhysicalExpression>>,
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
                    .probe_on
                    .iter()
                    .map(|expr| expr.eval(&batch)?.into_array(batch.num_rows()))
                    .collect::<Result<Vec<_>>>()?;
                self.hashes_buffer.clear();
                self.hashes_buffer.resize(batch.num_rows(), 0);
                create_hashes(&keys, &mut self.hashes_buffer, &self.random_state)?;
                self.state = HashJoinStreamState::ProcessProbeBatch(ProcessProbeBatchState {
                    batch,
                    offset: (0, None),
                    joined_probe_index: None,
                });
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
        let (build_indices, probe_indices, next_offset) = Self::get_matched_join_key_indices(
            map,
            self.build_on.as_slice(),
            build_batch,
            self.probe_on.as_slice(),
            &probe_batch.batch,
            &self.hashes_buffer,
            self.batch_size,
            probe_batch.offset,
        )?;

        let (build_indices, probe_indices) = Self::apply_join_filter(
            &self.filter,
            build_indices,
            probe_indices,
            build_batch,
            &probe_batch.batch,
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
            &probe_batch.batch,
            &build_indices,
            &probe_indices,
            &self.column_indices,
            JoinSide::Left,
        )?;

        let joined_probe_index = match probe_indices.len() {
            0 => None,
            n => Some(probe_indices.value(n - 1) as usize),
        };

        if let Some(next_offset) = next_offset {
            probe_batch.advance(next_offset, joined_probe_index)
        } else {
            self.state = HashJoinStreamState::FetchProbeBatch;
        }

        Ok(StreamResultState::Ready(Some(result)))
    }

    #[allow(clippy::too_many_arguments)]
    fn get_matched_join_key_indices(
        map: &JoinHashMap,
        build_on: &[Arc<dyn PhysicalExpression>],
        build_batch: &RecordBatch,
        probe_on: &[Arc<dyn PhysicalExpression>],
        probe_batch: &RecordBatch,
        hashes_buffer: &[u64],
        batch_size: usize,
        offset: JoinHashMapOffset,
    ) -> Result<(UInt64Array, UInt32Array, Option<JoinHashMapOffset>)> {
        let (mut probe_indices, mut build_indices, next_offset) =
            map.get_matched_indices(hashes_buffer, offset, batch_size);
        let build_indices: UInt64Array = PrimitiveArray::new(build_indices.finish().into(), None);
        let probe_indices: UInt32Array = PrimitiveArray::new(probe_indices.finish().into(), None);

        let (build_indices, probe_indices) = Self::apply_row_equality_filter(
            build_on,
            &build_indices,
            build_batch,
            probe_on,
            &probe_indices,
            probe_batch,
        )?;

        Ok((build_indices, probe_indices, next_offset))
    }

    fn apply_row_equality_filter(
        build_on: &[Arc<dyn PhysicalExpression>],
        build_indices: &UInt64Array,
        build_batch: &RecordBatch,
        probe_on: &[Arc<dyn PhysicalExpression>],
        probe_indices: &UInt32Array,
        probe_batch: &RecordBatch,
    ) -> Result<(UInt64Array, UInt32Array)> {
        let build_arrays = build_on
            .iter()
            .map(|expr| expr.eval(build_batch)?.into_array(build_batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        let probe_arrays = probe_on
            .iter()
            .map(|expr| expr.eval(probe_batch)?.into_array(probe_batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;

        let mut iter = build_arrays.iter().zip(probe_arrays.iter());

        let (first_build, first_probe) = iter.next().ok_or_else(|| Error::InvalidData {
            message:
                "At least one build and probe array should be provided for an equality comparison"
                    .to_string(),
            location: location!(),
        })?;

        let build_arr = take(first_build.as_ref(), &build_indices, None)?;
        let probe_arr = take(first_probe.as_ref(), &probe_indices, None)?;
        let mut equal_arr: BooleanArray = eq(&build_arr, &probe_arr)?;
        equal_arr = iter
            .map(|(build, probe)| {
                let build_arr = take(build.as_ref(), &build_indices, None)?;
                let probe_arr = take(probe.as_ref(), &probe_indices, None)?;
                eq(&build_arr, &probe_arr)
            })
            .try_fold(equal_arr, |acc, curr| and(&acc, &curr?))?;

        let equal_filter_builder = FilterBuilder::new(&equal_arr).optimize().build();
        let build_filtered = equal_filter_builder.filter(&build_indices)?;
        let probe_filtered = equal_filter_builder.filter(&probe_indices)?;

        Ok((
            downcast_array(build_filtered.as_ref()),
            downcast_array(probe_filtered.as_ref()),
        ))
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
                filter.schema(),
                build_batch,
                probe_batch,
                &build_indices,
                &probe_indices,
                filter.column_indices(),
                side,
            )?;
            let filter_result = filter
                .expression()
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
            let array = match col_idx.side() == side {
                true => {
                    let array = build_batch.column(col_idx.index());
                    if array.is_empty() || build_indices.null_count() == build_indices.len() {
                        new_null_array(array.data_type(), build_indices.len())
                    } else {
                        compute::take(array.as_ref(), build_indices, None)?
                    }
                }
                false => {
                    let array = probe_batch.column(col_idx.index());
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

    use arrow::compute::{self, concat_batches};
    use arrow_array::{PrimitiveArray, UInt32Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

    use crate::{
        expression::{
            operator::Operator,
            physical::{binary::BinaryExpr, column::ColumnExpr, literal::LiteralExpr},
            values::ScalarValue,
        },
        io::reader::csv::{options::CsvFileOpenerConfig, DEFAULT_BATCH_SIZE},
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

    use super::{HashJoinExec, JoinFilter, JoinHashMap, JoinOn};

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

    #[test]
    fn test_join_hashmap() {
        let mut hashmap = JoinHashMap::with_capacity(4);
        let values: [(usize, u64); 4] = [(1, 10), (2, 20), (3, 10), (4, 10)];
        hashmap.update_from_iter(values.iter().map(|(i, v)| (*i, v)));

        // map:
        // ---------
        // | 10 | 5 |
        // | 20 | 3 |
        // ---------
        // next:
        // ---------------------
        // | 0 | 0 | 0 | 2 | 4 |
        // ---------------------
        assert_eq!(hashmap.map.len(), 2);
        assert_eq!(hashmap.next.len(), 5);
        assert_eq!(hashmap.next[4], 4);
        assert_eq!(hashmap.next[3], 2);

        let (mut input_indices, mut match_indices, next_offset) =
            hashmap.get_matched_indices(&[10, 20], (0, None), DEFAULT_BATCH_SIZE);
        let build_indices: UInt64Array = PrimitiveArray::new(match_indices.finish().into(), None);
        let probe_indices: UInt32Array = PrimitiveArray::new(input_indices.finish().into(), None);

        assert_eq!(build_indices.len(), 4);
        assert_eq!(probe_indices.len(), 4);
        assert_eq!(compute::sum(&build_indices), Some(10));
        assert_eq!(compute::sum(&probe_indices), Some(1));
        assert!(next_offset.is_none());
    }

    #[tokio::test]
    async fn test_inner_join_with_filter() {
        let on: JoinOn = vec![(
            Arc::new(ColumnExpr::new("l1", 0)),
            Arc::new(ColumnExpr::new("r1", 0)),
        )];
        let intermediate_schema =
            Arc::new(Schema::new(vec![Field::new("r2", DataType::Int64, true)]));
        let column_indices = vec![JoinColumnIndex::new(1, JoinSide::Right)];
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
        let column_indices = vec![JoinColumnIndex::new(1, JoinSide::Left)];
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

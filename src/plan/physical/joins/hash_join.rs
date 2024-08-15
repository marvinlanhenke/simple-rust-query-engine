use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
    task::{ready, Poll},
};

use ahash::RandomState;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
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

const DEFAULT_BATCH_SIZE: usize = 1024;

pub type JoinOn = Vec<(Arc<dyn PhysicalExpression>, Arc<dyn PhysicalExpression>)>;

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug)]
struct JoinLeftData {
    map: HashMap<u64, u64>,
    batch: RecordBatch,
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
    projection: Option<Vec<usize>>,
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
        projection: Option<Vec<usize>>,
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

        Self::is_valid_projection(&schema, projection.as_ref())?;

        Ok(Self {
            lhs,
            rhs,
            on,
            filter,
            join_type,
            schema,
            projection,
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

    /// Checks if the given projection is valid.
    fn is_valid_projection(schema: &Schema, projection: Option<&Vec<usize>>) -> Result<()> {
        match projection {
            Some(cols) => {
                if cols
                    .iter()
                    .max()
                    .map_or(false, |idx| *idx >= schema.fields().len())
                {
                    Err(Error::InvalidData {
                        message: format!(
                            "project index {} out of bounds",
                            cols.iter().max().unwrap()
                        ),
                        location: location!(),
                    })
                } else {
                    Ok(())
                }
            }
            None => Ok(()),
        }
    }

    async fn collect_left_input() -> Result<JoinLeftData> {
        // TODO: this is just dummy data
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
        ]);
        let utf_arr = Arc::new(StringArray::from(vec!["hello", "world"]));
        let int_arr1 = Arc::new(Int64Array::from(vec![1, 2]));
        let int_arr2 = Arc::new(Int64Array::from(vec![11, 22]));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![utf_arr, int_arr1, int_arr2])?;

        Ok(JoinLeftData {
            map: HashMap::new(),
            batch,
        })
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
        let projected_column_indices = match &self.projection {
            None => self.column_indices.clone(),
            Some(proj) => proj.iter().map(|i| self.column_indices[*i]).collect(),
        };

        let build_input_future = async move { Self::collect_left_input().await }.boxed();

        let stream = HashJoinStream {
            schema: self.schema(),
            on_left,
            on_right,
            filter: self.filter.clone(),
            join_type: self.join_type,
            build_side: BuildSideState::Initial(build_input_future),
            probe_input,
            column_indices: projected_column_indices,
            state: HashJoinStreamState::WaitBuildSide,
            batch_size: DEFAULT_BATCH_SIZE,
            hashes: vec![],
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

type JoinHashMapOffset = (usize, Option<u64>);

struct ProcessProbeBatchState {
    batch: RecordBatch,
    offset: JoinHashMapOffset,
    joined_probe_index: Option<usize>,
}

impl ProcessProbeBatchState {
    fn advance(&mut self, offset: JoinHashMapOffset, joined_probe_index: Option<usize>) {
        self.offset = offset;
        if joined_probe_index.is_some() {
            self.joined_probe_index = joined_probe_index
        }
    }
}

type BuildInputFuture = BoxFuture<'static, Result<JoinLeftData>>;

enum BuildSideState {
    Initial(BuildInputFuture),
    Ready(Arc<JoinLeftData>),
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
    ProcessProbeBatch(ProcessProbeBatchState),
    ExhaustedProbeSide,
    Completed,
}

struct HashJoinStream {
    /// The input schema.
    schema: SchemaRef,
    on_left: Vec<Arc<dyn PhysicalExpression>>,
    on_right: Vec<Arc<dyn PhysicalExpression>>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    build_side: BuildSideState,
    /// The right (probe) input stream.
    probe_input: RecordBatchStream,
    column_indices: Vec<JoinColumnIndex>,
    state: HashJoinStreamState,
    /// Maximum output batch size.
    batch_size: usize,
    /// Internal buffer for computing hashes.
    hashes: Vec<u64>,
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
                    handle_stream_result!(self.process_probe_batch(cx))
                }
                ExhaustedProbeSide => {
                    handle_stream_result!(self.process_unmatched_build_batch(cx))
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
        self.build_side = BuildSideState::Ready(Arc::new(left_data));

        Poll::Ready(Ok(StreamResultState::Continue))
    }

    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StreamResultState<Option<RecordBatch>>>> {
        todo!()
    }

    fn process_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Result<StreamResultState<Option<RecordBatch>>> {
        todo!()
    }

    fn process_unmatched_build_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Result<StreamResultState<Option<RecordBatch>>> {
        todo!()
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

    use crate::{
        expression::physical::column::ColumnExpr,
        io::reader::csv::options::CsvFileOpenerConfig,
        plan::{
            logical::join::JoinType,
            physical::{plan::ExecutionPlan, scan::csv::CsvExec},
        },
        tests::create_schema,
    };

    use super::{HashJoinExec, JoinOn};

    #[tokio::test]
    async fn test_dummy() {
        let schema = Arc::new(create_schema());
        let lhs = CsvExec::new(
            "testdata/csv/simple.csv",
            CsvFileOpenerConfig::new(schema.clone()),
        );
        let rhs = CsvExec::new(
            "testdata/csv/simple.csv",
            CsvFileOpenerConfig::new(schema.clone()),
        );
        let on: JoinOn = vec![(
            Arc::new(ColumnExpr::new("c1", 0)),
            Arc::new(ColumnExpr::new("c1", 0)),
        )];
        let exec =
            HashJoinExec::try_new(Arc::new(lhs), Arc::new(rhs), on, None, JoinType::Left, None)
                .unwrap();
        let _ = exec.execute().unwrap();
    }

    #[test]
    fn test_is_valid_projection() {
        let schema = create_schema();

        let result = HashJoinExec::is_valid_projection(&schema, Some(&vec![0, 1, 2]));
        assert!(result.is_ok());

        let result = HashJoinExec::is_valid_projection(&schema, Some(&vec![0, 1, 4]));
        assert!(result.is_err());
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

use std::{any::Any, collections::HashSet, fmt::Display, sync::Arc, task::Poll};

use ahash::RandomState;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use futures::Stream;
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{
        logical::column::Column,
        physical::{column::ColumnExpr, expr::PhysicalExpression},
    },
    io::RecordBatchStream,
    plan::{
        logical::join::JoinType,
        physical::plan::{format_exec, ExecutionPlan},
    },
};

pub type JoinOn = Vec<(Arc<dyn PhysicalExpression>, Arc<dyn PhysicalExpression>)>;

#[derive(Debug)]
pub enum JoinSide {
    Left,
    Right,
}

#[derive(Debug)]
pub struct JoinColumnIndex {
    index: usize,
    side: JoinSide,
}

#[derive(Debug)]
pub struct JoinFilter {
    schema: SchemaRef,
    expression: Arc<dyn PhysicalExpression>,
    column_indices: Vec<JoinColumnIndex>,
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

    fn create_join_schema(
        left_schema: &Schema,
        right_schema: &Schema,
        join_type: &JoinType,
    ) -> (SchemaRef, Vec<JoinColumnIndex>) {
        todo!()
    }

    fn is_valid_projection(schema: &Schema, projection: Option<&Vec<usize>>) -> Result<()> {
        todo!()
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

    fn execute(&self) -> Result<crate::io::RecordBatchStream> {
        todo!()
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

enum BuildSideState {
    Initial,
    Ready,
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
    on: JoinOn,
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

    use crate::{expression::physical::column::ColumnExpr, tests::create_schema};

    use super::HashJoinExec;

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

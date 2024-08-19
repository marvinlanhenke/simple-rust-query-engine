use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{as_boolean_array, downcast_array},
    compute,
};
use arrow_array::{
    new_null_array, Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array, UInt64Array,
};
use arrow_schema::{Schema, SchemaBuilder, SchemaRef};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::physical::{column::ColumnExpr, expr::PhysicalExpression},
    plan::logical::join::JoinType,
};

/// Type alias representing a vector of pairs of physical expressions used for join conditions.
/// Each pair corresponds to a condition between the left and right sides of the join.
pub type JoinOn = Vec<(Arc<dyn PhysicalExpression>, Arc<dyn PhysicalExpression>)>;

/// Enum representing which side of a join a column belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    /// The left side of the join.
    Left,
    /// The right side of the join.
    Right,
}

/// Represents an index of a column involved in a join operation,
/// along with the side (left or right) from which the column originates.
#[derive(Debug, Clone, Copy)]
pub struct JoinColumnIndex {
    /// The column's index.
    index: usize,
    /// The side of the join from which the
    /// column originates.
    side: JoinSide,
}

impl JoinColumnIndex {
    /// Creates a new [`JoinColumnIndex`] instance.
    pub fn new(index: usize, side: JoinSide) -> Self {
        Self { index, side }
    }

    /// Returns the index of the column within the schema.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns the side of the join to which the column belongs.
    pub fn side(&self) -> JoinSide {
        self.side
    }
}

/// Represents a filter applied to a join operation.
#[derive(Debug, Clone)]
pub struct JoinFilter {
    /// The intermediate filter schema.
    schema: SchemaRef,
    /// The filter expression.
    expression: Arc<dyn PhysicalExpression>,
    /// The column indices (including the join side)
    /// used in the filter expression.
    column_indices: Vec<JoinColumnIndex>,
}

impl JoinFilter {
    /// Creates a new [`JoinFilter`] instance.
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

    /// The intermediate filter schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The filter expression.
    pub fn expression(&self) -> Arc<dyn PhysicalExpression> {
        self.expression.clone()
    }

    /// The column indices used in the filter expression.
    pub fn column_indices(&self) -> &[JoinColumnIndex] {
        self.column_indices.as_slice()
    }
}

/// Validates the join conditions by checking that the columns required for the join exist in both schemas.
pub fn is_valid_join(left_schema: &Schema, right_schema: &Schema, on: &JoinOn) -> Result<()> {
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

/// Creates the schema for the output of a join operation, starting with the fields from the left side.
pub fn create_join_schema(
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
                .map(|(index, field)| (field.clone(), JoinColumnIndex::new(index, JoinSide::Left)));
            let right_fields = right_schema
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        expression::physical::column::ColumnExpr,
        plan::{
            logical::join::JoinType,
            physical::joins::utils::{create_join_schema, is_valid_join},
        },
        tests::create_schema,
    };

    #[test]
    fn test_create_join_schema() {
        let left_schema = create_schema();
        let right_schema = create_schema();

        let (schema, column_indices) =
            create_join_schema(&left_schema, &right_schema, &JoinType::Inner);
        assert_eq!(schema.fields().len(), 6);
        assert_eq!(column_indices.len(), 6);
    }

    #[test]
    fn test_is_valid_join() {
        let left_schema = create_schema();
        let right_schema = create_schema();

        let result = is_valid_join(
            &left_schema,
            &right_schema,
            &vec![(
                Arc::new(ColumnExpr::new("c1", 0)),
                Arc::new(ColumnExpr::new("c1", 0)),
            )],
        );
        assert!(result.is_ok());

        let result = is_valid_join(
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

        let result = is_valid_join(
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

/// Applies the join filter to the matched build and probe indices.
///
/// This method evaluates the join filter expression on the intermediate batch and applies
/// the resulting boolean mask to the matched build and probe indices.
pub fn apply_join_filter(
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
        // Build intermediate batch and apply filter expression
        let intermediate_batch = build_batch_from_indices(
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

/// Constructs a record batch from the given build and probe indices.
///
/// This method takes the matched indices from the build and probe sides and constructs
/// the final output batch by combining the relevant columns.
pub fn build_batch_from_indices(
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

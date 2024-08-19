use std::{collections::HashSet, sync::Arc};

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

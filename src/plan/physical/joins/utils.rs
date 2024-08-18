use std::sync::Arc;

use arrow_schema::SchemaRef;

use crate::expression::physical::expr::PhysicalExpression;

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

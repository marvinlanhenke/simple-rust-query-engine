use std::sync::Arc;

use arrow_schema::SchemaRef;

use crate::expression::physical::expr::PhysicalExpression;

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

impl JoinColumnIndex {
    pub fn new(index: usize, side: JoinSide) -> Self {
        Self { index, side }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn side(&self) -> JoinSide {
        self.side
    }
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

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn expression(&self) -> Arc<dyn PhysicalExpression> {
        self.expression.clone()
    }

    pub fn column_indices(&self) -> &[JoinColumnIndex] {
        self.column_indices.as_slice()
    }
}

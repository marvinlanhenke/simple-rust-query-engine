use std::{fmt::Display, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{expression::logical::expr::Expression, utils::project_schema};

use super::plan::LogicalPlan;

/// Represents a projection operation in a logical plan.
#[derive(Debug, Clone)]
pub struct Projection {
    /// The input [`LogicalPlan`].
    input: Arc<LogicalPlan>,
    /// A list of expressions to apply.
    expression: Vec<Expression>,
    /// The projected schema.
    schema: SchemaRef,
}

impl Projection {
    /// Creates a new [`Projection`] instance.
    pub fn new(input: Arc<LogicalPlan>, expression: Vec<Expression>) -> Self {
        let schema = Arc::new(project_schema(&input.schema(), &expression));

        Self {
            input,
            expression,
            schema,
        }
    }

    /// A reference to the input [`LogicalPlan`].
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// A reference-counted [`arrow::datatypes::Schema`] of the data source.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Retrieves the child logical plans.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    /// Retrieves the filter expressions applied to [`Projection`].
    pub fn expressions(&self) -> &[Expression] {
        self.expression.as_slice()
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let exprs = self
            .expression
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Projection: [{}]", exprs)
    }
}

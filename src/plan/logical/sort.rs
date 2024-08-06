use std::{fmt::Display, sync::Arc};

use arrow_schema::SchemaRef;

use crate::expression::logical::expr::Expression;

use super::plan::LogicalPlan;

/// Represents a sort operation in a logical plan.
#[derive(Debug)]
pub struct Sort {
    /// The input [`LogicalPlan`].
    input: Arc<LogicalPlan>,
    /// The sort expressions.
    expression: Vec<Expression>,
}

impl Sort {
    /// Creates a new [`Sort`] instance.
    pub fn new(input: Arc<LogicalPlan>, expression: Vec<Expression>) -> Self {
        Self { input, expression }
    }

    /// Retrieves the input [`LogicalPlan`].
    pub fn input(&self) -> &LogicalPlan {
        self.input.as_ref()
    }

    /// Retrieves the sort expression.
    pub fn expressions(&self) -> &[Expression] {
        self.expression.as_slice()
    }

    /// A reference-counted [`arrow::datatypes::Schema`] of the input plan.
    pub fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Retrieves the child logical plans.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
}

impl Display for Sort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expr_str = self
            .expression
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Sort: [{}]", expr_str)
    }
}

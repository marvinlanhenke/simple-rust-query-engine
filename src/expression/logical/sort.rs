use std::{fmt::Display, sync::Arc};

use super::expr::Expression;

/// Represents a [`Sort`] expression in an AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Sort {
    /// The expression to evaluate.
    expression: Arc<Expression>,
    /// Indicates whether the sort direction is ascending or not.
    ascending: bool,
}

impl Sort {
    /// Creates a new [`Sort`] instance.
    pub fn new(expression: Arc<Expression>, ascending: bool) -> Self {
        Self {
            expression,
            ascending,
        }
    }

    /// The expression to evaluate.
    pub fn expression(&self) -> &Expression {
        self.expression.as_ref()
    }

    /// Indicates whether the sort direction is ascending or not.
    pub fn ascending(&self) -> bool {
        self.ascending
    }
}

impl Display for Sort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sort[{}]", self.expression)
    }
}

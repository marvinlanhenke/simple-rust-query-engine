use std::{fmt::Display, sync::Arc};

use crate::expression::operator::Operator;

use super::expr::Expression;

/// Represents a binary expression in the logical plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Binary {
    /// The left [`Expression`].
    lhs: Arc<Expression>,
    /// The [`Operator`] for the expression.
    op: Operator,
    /// The right [`Expression`].
    rhs: Arc<Expression>,
}

impl Binary {
    /// Creates a new [`Binary`] instance.
    pub fn new(lhs: Arc<Expression>, op: Operator, rhs: Arc<Expression>) -> Self {
        Self { lhs, op, rhs }
    }

    /// A reference to the left [`Expression`].
    pub fn lhs(&self) -> &Expression {
        &self.lhs
    }

    /// A reference to the [`Operator`].
    pub fn op(&self) -> &Operator {
        &self.op
    }

    /// A reference to the right [`Expression`].
    pub fn rhs(&self) -> &Expression {
        &self.rhs
    }
}

impl Display for Binary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write_child(f: &mut std::fmt::Formatter<'_>, expr: &Expression) -> std::fmt::Result {
            match expr {
                Expression::Binary(child) => write!(f, "{}", child),
                _ => write!(f, "{}", expr),
            }
        }
        write_child(f, &self.lhs)?;
        write!(f, " {} ", self.op)?;
        write_child(f, &self.rhs)
    }
}

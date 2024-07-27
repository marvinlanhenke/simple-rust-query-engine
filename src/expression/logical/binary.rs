use std::{fmt::Display, sync::Arc};

use crate::expression::operator::Operator;

use super::expr::Expression;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Binary {
    lhs: Arc<Expression>,
    op: Operator,
    rhs: Arc<Expression>,
}

impl Binary {
    pub fn new(lhs: Arc<Expression>, op: Operator, rhs: Arc<Expression>) -> Self {
        Self { lhs, op, rhs }
    }

    pub fn lhs(&self) -> &Expression {
        &self.lhs
    }

    pub fn op(&self) -> &Operator {
        &self.op
    }

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

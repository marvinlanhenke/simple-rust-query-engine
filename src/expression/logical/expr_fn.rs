use super::{column::Column, expr::Expression};

/// Creates an [`Expression::Column`] with provided `name`.
pub fn col(name: impl Into<String>) -> Expression {
    Expression::Column(Column::new(name))
}

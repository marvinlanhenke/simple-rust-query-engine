use std::collections::HashSet;

use crate::expression::logical::{column::Column, expr::Expression};

/// Recursively collect all referenced columns from the expression.
pub fn collect_columns(expr: &Expression, columns: &mut HashSet<Column>) {
    use Expression::*;

    match expr {
        Column(e) => {
            columns.insert(e.clone());
        }
        Binary(e) => {
            collect_columns(e.lhs(), columns);
            collect_columns(e.rhs(), columns);
        }
        _ => {}
    }
}

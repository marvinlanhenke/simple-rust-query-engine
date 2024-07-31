use std::sync::Arc;

use crate::expression::{operator::Operator, values::ScalarValue};

use super::{
    aggregate::{Aggregate, AggregateFunction},
    binary::Binary,
    column::Column,
    expr::Expression,
};

/// Creates an [`Expression::Column`] with provided `name`.
pub fn col(name: impl Into<String>) -> Expression {
    Expression::Column(Column::new(name))
}

/// Creates an [`Expression::Binary`] with provided expressions and operator.
pub fn binary_expr(lhs: Expression, op: Operator, rhs: Expression) -> Expression {
    Expression::Binary(Binary::new(Arc::new(lhs), op, Arc::new(rhs)))
}

macro_rules! make_aggregate_expr {
    ($name:ident, $f:ident) => {
        pub fn $name(expr: Expression) -> Expression {
            Expression::Aggregate(Aggregate::new(AggregateFunction::$f, Arc::new(expr)))
        }
    };
}

make_aggregate_expr!(count, Count);
make_aggregate_expr!(sum, Sum);
make_aggregate_expr!(avg, Avg);

/// Creates an [`Expression::Literal`].
pub fn lit<T: LiteralExt>(value: T) -> Expression {
    value.lit()
}

/// An extension trait for returning [`Expression::Literal`].
pub trait LiteralExt {
    fn lit(&self) -> Expression;
}

macro_rules! make_lit {
    ($ty:ident, $scalar:ident) => {
        impl LiteralExt for $ty {
            fn lit(&self) -> Expression {
                Expression::Literal(ScalarValue::$scalar(Some(*self)))
            }
        }
    };
}

make_lit!(i8, Int8);
make_lit!(i16, Int16);
make_lit!(i32, Int32);
make_lit!(i64, Int64);
make_lit!(u8, UInt8);
make_lit!(u16, UInt16);
make_lit!(u32, UInt32);
make_lit!(u64, UInt64);

impl LiteralExt for String {
    fn lit(&self) -> Expression {
        Expression::Literal(ScalarValue::Utf8(Some(self.clone())))
    }
}

impl LiteralExt for &String {
    fn lit(&self) -> Expression {
        Expression::Literal(ScalarValue::Utf8(Some(self.to_string())))
    }
}

impl LiteralExt for &str {
    fn lit(&self) -> Expression {
        Expression::Literal(ScalarValue::Utf8(Some(self.to_string())))
    }
}

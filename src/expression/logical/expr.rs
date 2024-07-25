use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expression {
    Column,
    ScalarValue,
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expression::*;

        match self {
            Column => write!(f, "TODO!"),
            ScalarValue => write!(f, "TODO!"),
        }
    }
}

use std::fmt::Display;

// An enumeration of operators that can be applied to an [`Expression`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    And,
    Or,
}

impl Operator {
    /// Determines if the operator is a logical operator (e.g. `And`).
    pub fn is_logical_operator(&self) -> bool {
        use Operator::*;
        matches!(self, Or | And)
    }

    /// Determines if the operator is a comparison operator (e.g. `Eq`).
    pub fn is_comparision_operator(&self) -> bool {
        use Operator::*;
        matches!(self, Eq | NotEq | Lt | LtEq | Gt | GtEq)
    }

    /// Determines if the operator is a numeric operator (e.g. `Plus`).
    pub fn is_numerical_operator(&self) -> bool {
        use Operator::*;
        matches!(self, Plus | Minus | Multiply | Divide)
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Operator::*;

        match self {
            Eq => write!(f, "="),
            NotEq => write!(f, "!="),
            Lt => write!(f, "<"),
            LtEq => write!(f, "<="),
            Gt => write!(f, ">"),
            GtEq => write!(f, ">="),
            Plus => write!(f, "+"),
            Minus => write!(f, "-"),
            Multiply => write!(f, "*"),
            Divide => write!(f, "/"),
            And => write!(f, "AND"),
            Or => write!(f, "OR"),
        }
    }
}

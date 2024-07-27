use std::fmt::Display;

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
    pub fn is_logical_operator(&self) -> bool {
        use Operator::*;
        matches!(self, Or | And)
    }

    pub fn is_comparision_operator(&self) -> bool {
        use Operator::*;
        matches!(self, Eq | NotEq | Lt | LtEq | Gt | GtEq)
    }

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

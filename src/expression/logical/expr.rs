use std::fmt::{Display, Write};

use crate::{
    error::{Error, Result},
    expression::{coercion::Signature, operator::Operator, values::ScalarValue},
    plan::logical::plan::LogicalPlan,
};
use arrow::{
    compute::{cast_with_options, CastOptions},
    datatypes::{DataType, Field, Schema},
};
use snafu::location;

use super::{
    aggregate::Aggregate, binary::Binary, column::Column, expr_fn::binary_expr, sort::Sort,
};

/// Represents a logical [`Expression`] in an AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expression {
    /// A [`Column`] expression.
    Column(Column),
    /// A [`ScalarValue`] expression.
    Literal(ScalarValue),
    /// A [`Binary`] expression.
    Binary(Binary),
    /// An [`Aggregate`] expression.
    Aggregate(Aggregate),
    /// A [`Sort`] expression.
    Sort(Sort),
}

impl Expression {
    /// Returns the name of this expression as it should appear in a schema.
    pub fn display_name(&self) -> Result<String> {
        let mut name = String::new();
        Self::display_name_impl(&mut name, self)?;
        Ok(name)
    }

    /// Resolves this column to its [`Field`] definition from a logical plan.
    pub fn to_field(&self, plan: &LogicalPlan) -> Result<Field> {
        use Expression::*;

        match self {
            Column(e) => e.to_field_from_plan(plan),
            _ => {
                let data_type = self.data_type(&plan.schema())?;
                Ok(Field::new(self.display_name()?, data_type, true))
            }
        }
    }

    /// Returns the [`DataType`] of the expression.
    pub fn data_type(&self, schema: &Schema) -> Result<DataType> {
        use Expression::*;

        match self {
            Column(e) => Ok(e.to_field(schema)?.data_type().clone()),
            Literal(e) => Ok(e.data_type()),
            Binary(e) => {
                let lhs = e.lhs().data_type(schema)?;
                let rhs = e.rhs().data_type(schema)?;
                Signature::get_result_type(&lhs, e.op(), &rhs)
            }
            Aggregate(e) => e.result_type(),
            Sort(e) => e.expression().data_type(schema),
        }
    }

    /// Attempts to cast the current expression to a specified data type.
    ///
    /// This function checks if the expression's current data type matches the desired `to_type`.
    /// If the types match, it returns the expression unchanged. If they differ, it attempts to cast
    /// the expression to the new type. Currently, only literal expressions are supported for casting.
    /// For literals, the function uses `cast_with_options` to perform the cast and returns a new
    /// literal expression with the cast result.
    pub fn cast_to(&self, to_type: &DataType, schema: &Schema) -> Result<Expression> {
        use Expression::*;

        let original_type = self.data_type(schema)?;

        if original_type == *to_type {
            return Ok(self.clone());
        };

        match self {
            Literal(scalar) => {
                let cast_options = CastOptions {
                    safe: false,
                    format_options: Default::default(),
                };
                let array = cast_with_options(&scalar.to_array(1), to_type, &cast_options)?;

                Ok(Expression::Literal(ScalarValue::try_from_array(&array, 0)?))
            }
            _ => Ok(self.clone()),
        }
    }

    /// Generates a display name for an expression by writing it to the provided writer.
    ///
    /// This function traverses the expression tree and writes a human-readable representation
    /// of the expression to the given writer.
    fn display_name_impl<W: Write>(w: &mut W, expr: &Expression) -> Result<()> {
        use Expression::*;

        match expr {
            Column(e) => write!(w, "{}", e)?,
            Literal(e) => write!(w, "{}", e)?,
            Binary(e) => {
                Self::display_name_impl(w, e.lhs())?;
                write!(w, " {} ", e.op())?;
                Self::display_name_impl(w, e.rhs())?;
            }
            Aggregate(e) => write!(w, "{}({})", e.name(), e.expression())?,
            Sort(_) => {
                return Err(Error::InvalidOperation {
                    message: "Display name is not supported for Sort expressions".to_string(),
                    location: location!(),
                })
            }
        };

        Ok(())
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expression::*;

        match self {
            Column(e) => write!(f, "{}", e),
            Literal(e) => write!(f, "{}", e),
            Binary(e) => write!(f, "{}", e),
            Aggregate(e) => write!(f, "{}", e),
            Sort(e) => write!(f, "{}", e),
        }
    }
}

macro_rules! make_expr_fn {
    ($fn:ident, $op:ident) => {
        impl Expression {
            pub fn $fn(self, other: Expression) -> Expression {
                binary_expr(self, Operator::$op, other)
            }
        }
    };
}

make_expr_fn!(eq, Eq);
make_expr_fn!(neq, NotEq);
make_expr_fn!(lt, Lt);
make_expr_fn!(lt_eq, LtEq);
make_expr_fn!(gt, Gt);
make_expr_fn!(gt_eq, GtEq);
make_expr_fn!(and, And);
make_expr_fn!(or, Or);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::{
        expression::{
            logical::{binary::Binary, column::Column},
            operator::Operator,
            values::ScalarValue,
        },
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        plan::logical::{plan::LogicalPlan, scan::Scan},
        tests::create_schema,
    };

    use super::Expression;

    #[test]
    fn test_expr_to_field() {
        let schema = Arc::new(create_schema());
        let options = CsvReadOptions::builder().with_schema(Some(schema)).build();
        let source = CsvDataSource::try_new("test_path", options).unwrap();
        let plan = LogicalPlan::Scan(Scan::new("test_path", Arc::new(source), None, vec![]));

        let cols = [
            ("c1", DataType::Utf8, Expression::Column(Column::new("c1"))),
            ("c2", DataType::Int64, Expression::Column(Column::new("c2"))),
            ("c3", DataType::Int64, Expression::Column(Column::new("c3"))),
        ];

        for (name, data_type, col) in cols.iter() {
            let field = col.to_field(&plan).unwrap();
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), data_type);
        }
    }

    #[test]
    fn test_expr_data_type() {
        let schema = create_schema();
        let column = Arc::new(Expression::Column(Column::new("c1")));
        let literal = Arc::new(Expression::Literal(ScalarValue::Utf8(Some(
            "a".to_string(),
        ))));
        let binary = Expression::Binary(Binary::new(column.clone(), Operator::Eq, literal.clone()));

        let result = column.data_type(&schema).unwrap();
        assert_eq!(result, DataType::Utf8);

        let result = literal.data_type(&schema).unwrap();
        assert_eq!(result, DataType::Utf8);

        let result = binary.data_type(&schema).unwrap();
        assert_eq!(result, DataType::Boolean);
    }
}

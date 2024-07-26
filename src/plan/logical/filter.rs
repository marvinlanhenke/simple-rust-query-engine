use std::fmt::Display;

use arrow::datatypes::{DataType, SchemaRef};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::logical::expr::Expression,
};

use super::plan::LogicalPlan;

/// Represents a filter operation in a logical plan.
#[derive(Debug)]
pub struct Filter {
    /// The input [`LogicalPlan`].
    input: Box<LogicalPlan>,
    /// The filter predicate to apply.
    predicate: Expression,
}

impl Filter {
    /// Attempts to create a new [`Filter`] instance.
    pub fn try_new(input: Box<LogicalPlan>, predicate: Expression) -> Result<Self> {
        if predicate.data_type(&input.schema())? != DataType::Boolean {
            return Err(Error::InvalidData {
                message: format!(
                    "Cannot create filter with non-boolean predicate '{}'",
                    predicate
                ),
                location: location!(),
            });
        };

        Ok(Self { input, predicate })
    }

    /// Retrieves the input [`LogicalPlan`].
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// Retrieves the filter predicate applied to [`Filter`].
    pub fn predicate(&self) -> &Expression {
        &self.predicate
    }

    /// A reference-counted [`arrow::datatypes::Schema`] of the input plan.
    pub fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Retrieves the child logical plans.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: [{}]", self.predicate)
    }
}

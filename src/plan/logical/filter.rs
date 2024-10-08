use std::{fmt::Display, sync::Arc};

use arrow::datatypes::{DataType, SchemaRef};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{coercion::coerce_binary_expression, logical::expr::Expression},
};

use super::plan::LogicalPlan;

/// Represents a filter operation in a logical plan.
#[derive(Debug, Clone)]
pub struct Filter {
    /// The input [`LogicalPlan`].
    input: Arc<LogicalPlan>,
    /// The filter predicate to apply.
    predicate: Expression,
}

impl Filter {
    /// Attempts to create a new [`Filter`] instance.
    pub fn try_new(input: Arc<LogicalPlan>, predicate: Expression) -> Result<Self> {
        let schema = input.schema();

        let coerced = coerce_binary_expression(&schema, &predicate)?;

        if coerced.data_type(&input.schema())? != DataType::Boolean {
            return Err(Error::InvalidData {
                message: format!(
                    "Cannot create filter with non-boolean predicate '{}'",
                    coerced
                ),
                location: location!(),
            });
        };

        Ok(Self {
            input,
            predicate: coerced,
        })
    }

    /// Retrieves the input [`LogicalPlan`].
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// Retrieves the filter predicate applied to [`Filter`].
    pub fn expressions(&self) -> &[Expression] {
        std::slice::from_ref(&self.predicate)
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

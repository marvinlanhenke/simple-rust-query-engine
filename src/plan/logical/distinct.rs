use std::{fmt::Display, sync::Arc};

use arrow_schema::SchemaRef;

use super::plan::LogicalPlan;

#[derive(Debug, Clone)]
pub struct Distinct {
    input: Arc<LogicalPlan>,
}

impl Distinct {
    /// Creates a new [`Distinct`] instance.
    pub fn new(input: Arc<LogicalPlan>) -> Self {
        Self { input }
    }

    /// Retrieves the input [`LogicalPlan`].
    pub fn input(&self) -> &LogicalPlan {
        &self.input
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

impl Display for Distinct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Distinct")
    }
}

use std::{fmt::Display, sync::Arc};

use arrow_schema::SchemaRef;

use super::plan::LogicalPlan;

/// Represents a logical plan node that limits the number of rows processed
/// by a query, potentially skipping a number of rows and fetching a maximum
/// number of rows from the input plan.
#[derive(Debug)]
pub struct Limit {
    /// The input [`LogicalPlan`].
    input: Arc<LogicalPlan>,
    /// The number of rows to skip before fetching starts.
    skip: usize,
    /// The maximum number of rows to fetch after skipping.
    /// If `None`, all rows after the skip will be fetched.
    fetch: Option<usize>,
}

impl Limit {
    /// Creates a new [`Limit`] instance.
    pub fn new(input: Arc<LogicalPlan>, skip: usize, fetch: Option<usize>) -> Self {
        Self { input, skip, fetch }
    }

    /// Retrieves the input [`LogicalPlan`].
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// The number of rows to skip before fetching starts.
    pub fn skip(&self) -> usize {
        self.skip
    }

    /// The maximum number of rows to fetch after skipping.
    /// If `None`, all rows after the skip will be fetched.
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
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

impl Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limit: [skip: {}, fetch:{:?}]", self.skip, self.fetch)
    }
}

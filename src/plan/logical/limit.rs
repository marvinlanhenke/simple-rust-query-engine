use std::{fmt::Display, sync::Arc};

use arrow_schema::SchemaRef;

use super::plan::LogicalPlan;

#[derive(Debug)]
pub struct Limit {
    input: Arc<LogicalPlan>,
    skip: usize,
    fetch: Option<usize>,
}

impl Limit {
    pub fn new(input: Arc<LogicalPlan>, skip: usize, fetch: Option<usize>) -> Self {
        Self { input, skip, fetch }
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub fn skip(&self) -> usize {
        self.skip
    }

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

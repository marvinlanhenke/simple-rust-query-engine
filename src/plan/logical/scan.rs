use std::{fmt::Display, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{expression::logical::expr::Expression, io::DataSource};

use super::plans::LogicalPlan;

#[derive(Debug)]
pub struct Scan {
    path: String,
    source: Arc<dyn DataSource>,
    projection: Option<Vec<String>>,
    filter: Vec<Expression>,
}

impl Scan {
    pub fn new(
        path: impl Into<String>,
        source: Arc<dyn DataSource>,
        projection: Option<Vec<String>>,
        filter: Vec<Expression>,
    ) -> Self {
        Self {
            path: path.into(),
            source,
            projection,
            filter,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    pub fn children(&self) -> &[&LogicalPlan] {
        &[]
    }

    pub fn expressions(&self) -> &[&Expression] {
        &[]
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.projection {
            None => write!(
                f,
                "Scan: {}; projection=None; filter=[{:?}]",
                self.path, self.filter
            ),
            Some(projection) => write!(
                f,
                "Scan: {}; projection={:?}; filter=[{:?}]",
                self.path, projection, self.filter
            ),
        }
    }
}

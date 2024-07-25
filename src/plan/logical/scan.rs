use std::{fmt::Display, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{expression::logical::expr::Expression, io::DataSource};

use super::plans::LogicalPlan;

/// A scan operation on a [`DataSource`].
#[derive(Debug)]
pub struct Scan {
    /// The filesystem path to the data file.
    path: String,
    /// A reference-counted [`DataSource`] to be scanned.
    source: Arc<dyn DataSource>,
    /// An optional list of column names to project.
    projection: Option<Vec<String>>,
    /// A list of filter expressions to apply.
    filter: Vec<Expression>,
}

impl Scan {
    /// Creates a new [`Scan`] instance.
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

    /// A reference-counted [`arrow::datatypes::Schema`] of the data source.
    pub fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    /// Retrieves the child logical plans.
    ///
    /// Since [`Scan`] has no children, this returns an empty slice.
    pub fn children(&self) -> &[&LogicalPlan] {
        &[]
    }

    /// Retrieves the filter expressions applied to [`Scan`].
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

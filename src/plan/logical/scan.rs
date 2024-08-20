use std::{fmt::Display, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{expression::logical::expr::Expression, io::DataSource};

use super::plan::LogicalPlan;

/// A scan operation on a [`DataSource`].
#[derive(Debug, Clone)]
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

    /// Retrieves the filesystem path to the data file.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// A reference-counted [`DataSource`].
    pub fn source(&self) -> Arc<dyn DataSource> {
        self.source.clone()
    }

    /// An optional list of column names to project
    pub fn projection(&self) -> Option<&Vec<String>> {
        self.projection.as_ref()
    }

    /// A reference-counted [`arrow::datatypes::Schema`] of the data source.
    pub fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    /// Retrieves the child logical plans.
    ///
    /// Since [`Scan`] has no children, this returns an empty slice.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    /// Retrieves the filter expressions applied to [`Scan`].
    pub fn expressions(&self) -> &[Expression] {
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

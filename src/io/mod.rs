use std::{fmt::Debug, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::stream::BoxStream;

use crate::{
    error::Result, expression::logical::expr::Expression, plan::physical::plan::ExecutionPlan,
};

pub mod reader;
pub mod writer;

/// A type alias for a pin-boxed, thread-safe stream of `RecordBatch`'es.
pub type RecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// A trait for opening and initializing readers.
///
/// Implementations of this trait should also provide functionality
/// to open a file from a given path and return a stream of `RecordBatch`'es.
pub trait FileOpener {
    /// Opens a file at the specified path and returns a future
    /// which resolves to a data stream returning `RecordBatch`es.
    fn open(&self, path: &str) -> Result<RecordBatchStream>;
}

/// Whether and how a filter predicate can be handled
/// by a [`DataSource`] for scan operations.
#[derive(Debug)]
pub enum PredicatePushDownSupport {
    Unsupported,
    Inexact,
    Exact,
}

/// A trait defining the capabilities of a [`DataSource`] provider.
pub trait DataSource: Debug + Send + Sync {
    /// A reference-counted [`arrow::datatypes::Schema`].
    fn schema(&self) -> SchemaRef;

    /// Creates an [`ExecutionPlan`] to scan the [`DataSource`].
    fn scan(&self, projection: Option<&Vec<String>>) -> Result<Arc<dyn ExecutionPlan>>;

    /// Whether the data source supports predicate pushdown, or not.
    fn can_pushdown_predicates(
        &self,
        expression: &[Expression],
    ) -> Result<Vec<PredicatePushDownSupport>> {
        expression
            .iter()
            .map(|_| Ok(PredicatePushDownSupport::Unsupported))
            .collect()
    }
}

use std::fmt::Debug;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::stream::BoxStream;

use crate::error::Result;

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

pub trait DataSource: Debug {
    /// A reference-counted [`arrow::datatypes::Schema`].
    fn schema(&self) -> SchemaRef;
    /// TODO: this should return the (physical) execution plan
    fn scan(&self) -> Result<()>;
}

use arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use futures::{future::BoxFuture, stream::BoxStream};

use crate::error::Result;

pub mod reader;
pub mod writer;

/// A type alias for a boxed future that opens a file and returns a stream.
pub type FileOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, std::result::Result<RecordBatch, ArrowError>>>>;

/// A trait for opening and initializing readers.
///
/// Implementations of this trait should provide functionality
/// to open a file from a given path and set up any necessary data stream or
/// reader based on the file's format.
pub trait FileOpener {
    /// Opens a file at the specified path and returns a future
    /// which resolves to a data stream returning `RecordBatch`es.
    fn open(&self, path: &str) -> Result<FileOpenFuture>;
}

pub trait DataSource {
    /// A reference-counted [`arrow::datatypes::Schema`].
    fn schema(&self) -> SchemaRef;
    /// TODO: this should return the (physical) execution plan
    fn scan(&self) -> Result<()>;
}

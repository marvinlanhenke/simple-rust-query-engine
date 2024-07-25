use std::{any::Any, fmt::Display};

use arrow::datatypes::SchemaRef;

use crate::{error::Result, io::RecordBatchStream};

/// A trait to represent an [`ExecutionPlan`] for query execution.
pub trait ExecutionPlan: Display {
    /// Returns a reference to self as a `dyn Any`.
    fn as_any(&self) -> &dyn Any;

    /// A reference-counted [`arrow::datatypes::Schema`].
    fn schema(&self) -> SchemaRef;

    /// Retrieves the child execution plans.
    fn children(&self) -> Vec<&dyn ExecutionPlan>;

    /// Executes the [`ExecutionPlan`] and returns a stream of `RecordBatch`'es.
    fn execute(&self) -> Result<RecordBatchStream>;
}

pub fn format_exec(
    input: &dyn ExecutionPlan,
    f: &mut std::fmt::Formatter<'_>,
    indent: usize,
) -> std::fmt::Result {
    for _ in 0..indent {
        write!(f, "\t")?;
    }
    write!(f, "{}", input)?;
    writeln!(f)?;

    for child in input.children() {
        format_exec(child, f, indent + 1)?;
    }

    Ok(())
}

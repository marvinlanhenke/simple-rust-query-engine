use std::{any::Any, fmt::Display};

use arrow::datatypes::SchemaRef;

use crate::{error::Result, io::RecordBatchStream};

/// A trait to represent an [`ExecutionPlan`] for query execution.
pub trait ExecutionPlan: Display {
    fn as_any(&self) -> &dyn Any;

    fn schema(&self) -> SchemaRef;

    fn children(&self) -> Vec<&dyn ExecutionPlan>;

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

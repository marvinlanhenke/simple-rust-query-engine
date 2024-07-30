use std::fmt::Display;

use arrow::datatypes::SchemaRef;

use super::{aggregate::Aggregate, filter::Filter, projection::Projection, scan::Scan};

/// Represents a [`LogicalPlan`] for query execution.
#[derive(Debug)]
pub enum LogicalPlan {
    /// A [`Scan`] operation on the [`DataSource`].
    Scan(Scan),
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
}

impl LogicalPlan {
    /// A reference-counted [`arrow::datatypes::Schema`].
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan(plan) => plan.schema(),
            LogicalPlan::Projection(plan) => plan.schema(),
            LogicalPlan::Filter(plan) => plan.schema(),
            LogicalPlan::Aggregate(plan) => plan.schema(),
        }
    }

    /// Retrieves the child logical plans.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(plan) => plan.children(),
            LogicalPlan::Projection(plan) => plan.children(),
            LogicalPlan::Filter(plan) => plan.children(),
            LogicalPlan::Aggregate(plan) => plan.children(),
        }
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_plan(self, f, 0)
    }
}

/// Formats the logical plan for display purposes with indentation.
pub fn format_plan(
    input: &LogicalPlan,
    f: &mut std::fmt::Formatter<'_>,
    indent: usize,
) -> std::fmt::Result {
    for _ in 0..indent {
        write!(f, "\t")?;
    }

    match input {
        LogicalPlan::Scan(plan) => write!(f, "{}", plan)?,
        LogicalPlan::Projection(plan) => write!(f, "{}", plan)?,
        LogicalPlan::Filter(plan) => write!(f, "{}", plan)?,
        LogicalPlan::Aggregate(plan) => write!(f, "{}", plan)?,
    }
    writeln!(f)?;

    for child in input.children() {
        format_plan(child, f, indent + 1)?;
    }

    Ok(())
}

use std::fmt::Display;

use arrow::datatypes::SchemaRef;

use super::{
    aggregate::Aggregate, filter::Filter, limit::Limit, projection::Projection, scan::Scan,
    sort::Sort,
};

/// Represents a [`LogicalPlan`] for query execution.
#[derive(Debug)]
pub enum LogicalPlan {
    /// Represents a data scanning operation from a specified data source.
    /// Typically used as the initial step in query processing to load data,
    /// e.g. `SELECT * FROM table;`
    Scan(Scan),
    /// Represents a projection operation, which constructs a new table that
    /// contains only specific fields from the original table.
    /// This is typically used to return a subset of columns,
    /// e.g. `SELECT c1, c2 FROM table;`
    Projection(Projection),
    /// Represents a filter operation, which is used to select rows from a table
    /// based on specific criteria. This is equivalent to the SQL `WHERE` clause,
    /// e.g. `SELECT * FROM table WHERE c1 > 5;`
    Filter(Filter),
    /// Represents an aggregate operation, which performs a calculation on a collection
    /// of values and returns a single value. Aggregations typically involve operations
    /// such as summing, averaging, or counting sets of values,
    /// e.g. `SELECT COUNT(c2), SUM(c2) FROM table GROUP BY c1;`
    Aggregate(Aggregate),
    /// Represents a sort operation, which arranges the rows in a table into a specified order.
    /// This is similar to the SQL `ORDER BY` clause,
    /// e.g. `SELECT * FROM table ORDER BY c1 ASC;`
    Sort(Sort),
    /// Represent a limit operation, skips some rows,
    /// and then fetches some number of rows.
    Limit(Limit),
}

impl LogicalPlan {
    /// A reference-counted [`arrow::datatypes::Schema`].
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan(plan) => plan.schema(),
            LogicalPlan::Projection(plan) => plan.schema(),
            LogicalPlan::Filter(plan) => plan.schema(),
            LogicalPlan::Aggregate(plan) => plan.schema(),
            LogicalPlan::Sort(plan) => plan.schema(),
            LogicalPlan::Limit(plan) => plan.schema(),
        }
    }

    /// Retrieves the child logical plans.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(plan) => plan.children(),
            LogicalPlan::Projection(plan) => plan.children(),
            LogicalPlan::Filter(plan) => plan.children(),
            LogicalPlan::Aggregate(plan) => plan.children(),
            LogicalPlan::Sort(plan) => plan.children(),
            LogicalPlan::Limit(plan) => plan.children(),
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
        LogicalPlan::Sort(plan) => write!(f, "{}", plan)?,
        LogicalPlan::Limit(plan) => write!(f, "{}", plan)?,
    }
    writeln!(f)?;

    for child in input.children() {
        format_plan(child, f, indent + 1)?;
    }

    Ok(())
}

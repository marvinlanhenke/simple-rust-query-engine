use std::{fmt::Display, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};

use crate::{error::Result, expression::logical::expr::Expression};

use super::plan::LogicalPlan;

/// Represents an [`Aggregate`] logical plan in a query.
#[derive(Debug)]
pub struct Aggregate {
    /// The input logical plan.
    input: Arc<LogicalPlan>,
    /// A reference-counted [`arrow::datatypes::Schema`].
    schema: SchemaRef,
    /// The expressions used for grouping the data.
    group_expressions: Vec<Expression>,
    /// The expressions used for aggregation.
    aggregate_expressions: Vec<Expression>,
}

impl Aggregate {
    /// Creates a new [`Aggregate`] instance.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expressions: Vec<Expression>,
        aggregate_expressions: Vec<Expression>,
    ) -> Result<Self> {
        let fields = group_expressions
            .iter()
            .chain(aggregate_expressions.iter())
            .map(|expr| expr.to_field(&input))
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            input,
            schema,
            group_expressions,
            aggregate_expressions,
        })
    }

    /// The input logical plan.
    pub fn input(&self) -> &LogicalPlan {
        self.input.as_ref()
    }

    /// A reference-counted [`arrow::datatypes::Schema`].
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the children of this logical plan.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Returns a slice of expressions used for grouping the data.
    pub fn group_expressions(&self) -> &[Expression] {
        self.group_expressions.as_slice()
    }

    /// Returns a slice of expressions used for aggregation.
    pub fn aggregate_expressions(&self) -> &[Expression] {
        self.aggregate_expressions.as_slice()
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let group_expr_str = self
            .group_expressions
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let aggr_expr_str = self
            .aggregate_expressions
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        write!(
            f,
            "Aggregate: groupExpr:[{}]; aggrExpr:[{}]",
            group_expr_str, aggr_expr_str
        )
    }
}
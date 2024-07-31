use std::sync::Arc;

use arrow::datatypes::Schema;
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{
        logical::{aggregate::AggregateFunction, expr::Expression},
        physical::{
            aggregate::{count::CountExpr, AggregateExpr},
            binary::BinaryExpr,
            column::ColumnExpr,
            expr::PhysicalExpression,
            literal::LiteralExpr,
        },
    },
    plan::{
        logical::plan::LogicalPlan,
        physical::{aggregate::AggregateExec, filter::FilterExec, projection::ProjectionExec},
    },
};

use super::{logical::aggregate::Aggregate, physical::plan::ExecutionPlan};

/// The query [`Planner`].
///
/// Responsible for translating logical to physical plans.
pub struct Planner;

impl Planner {
    /// Attempts to create a [`ExecutionPlan`] from the provided input [`LogicalPlan`].
    pub fn create_physical_plan(input: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        use LogicalPlan::*;

        match input {
            Scan(plan) => {
                let source = plan.source();
                let projection = plan.projection();
                source.scan(projection)
            }
            Projection(plan) => {
                let input = Self::create_physical_plan(plan.input())?;

                let mut fields = Vec::with_capacity(plan.expressions().len());
                let mut expression = Vec::with_capacity(plan.expressions().len());
                for curr_expr in plan.expressions().iter() {
                    let field = curr_expr.to_field(plan.input())?;
                    let expr = Self::create_physical_expression(plan.input(), curr_expr)?;
                    fields.push(field);
                    expression.push(expr);
                }
                let schema = Arc::new(Schema::new(fields));

                Ok(Arc::new(ProjectionExec::new(input, schema, expression)))
            }
            Filter(plan) => {
                let physical_input = Self::create_physical_plan(plan.input())?;
                let predicate = Self::create_physical_expression(input, &plan.expressions()[0])?;

                Ok(Arc::new(FilterExec::try_new(physical_input, predicate)?))
            }
            Aggregate(plan) => {
                let physical_input = Self::create_physical_plan(plan.input())?;

                // not supported yet
                let group_by = vec![];
                let aggregate_expressions = Self::create_aggregate_expression(plan)?;

                Ok(Arc::new(AggregateExec::try_new(
                    physical_input,
                    group_by,
                    aggregate_expressions,
                )?))
            }
        }
    }

    /// Creates aggregate expressions from aggregate plan and aggregate function.
    fn create_aggregate_expression(plan: &Aggregate) -> Result<Vec<Arc<dyn AggregateExpr>>> {
        let mut aggregate_expressions: Vec<Arc<dyn AggregateExpr>> =
            Vec::with_capacity(plan.aggregate_expressions().len());
        for expr in plan.aggregate_expressions().iter() {
            if let Expression::Aggregate(agg) = expr {
                let phys_expr = Self::create_physical_expression(plan.input(), agg.expression())?;
                let aggr_expr = match agg.func() {
                    AggregateFunction::Count => Arc::new(CountExpr::new(phys_expr)),
                    AggregateFunction::Sum => todo!(),
                };
                aggregate_expressions.push(aggr_expr);
            };
        }
        Ok(aggregate_expressions)
    }

    /// Converts a logical to a physical expression.
    fn create_physical_expression(
        input: &LogicalPlan,
        expr: &Expression,
    ) -> Result<Arc<dyn PhysicalExpression>> {
        use Expression::*;

        match expr {
            Column(v) => {
                let (index, _) = input.schema().column_with_name(v.name()).ok_or_else(|| {
                    Error::InvalidData {
                        message: format!(
                            "Column with name '{}' could not be found in schema",
                            v.name()
                        ),
                        location: location!(),
                    }
                })?;
                Ok(Arc::new(ColumnExpr::new(v.name(), index)))
            }
            Literal(v) => Ok(Arc::new(LiteralExpr::new(v.clone()))),
            Binary(v) => {
                let left = Self::create_physical_expression(input, v.lhs())?;
                let right = Self::create_physical_expression(input, v.rhs())?;
                Ok(Arc::new(BinaryExpr::new(left, v.op().clone(), right)))
            }
            other => Err(Error::InvalidOperation {
                message: format!(
                    "Conversion from logical to physical expression is not supported for {}",
                    other
                ),
                location: location!(),
            }),
        }
    }
}

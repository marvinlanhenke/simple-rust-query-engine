use std::sync::Arc;

use arrow::datatypes::Schema;
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{
        logical::{aggregate::AggregateFunction, expr::Expression},
        physical::{
            aggregate::{
                average::AvgExpr,
                count::CountExpr,
                min_max::{MaxExpr, MinExpr},
                sum::SumExpr,
                AggregateExpr,
            },
            binary::BinaryExpr,
            column::ColumnExpr,
            expr::PhysicalExpression,
            literal::LiteralExpr,
            sort::SortExpr,
        },
    },
    plan::{
        logical::plan::LogicalPlan,
        physical::{
            aggregate::AggregateExec,
            filter::FilterExec,
            joins::hash_join::{HashJoinExec, JoinOn},
            limit::LimitExec,
            projection::ProjectionExec,
            sorts::sort::SortExec,
        },
    },
};

use super::{logical::aggregate::Aggregate, physical::plan::ExecutionPlan};

macro_rules! make_aggregate_expr {
    ($e:expr, $plan:expr, $ty:ident) => {{
        let data_type = $e.data_type(&$plan.schema())?;
        Arc::new($ty::new($e, data_type))
    }};
}

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

                let mut group_by = Vec::with_capacity(plan.group_by().len());
                for expr in plan.group_by().iter() {
                    let phys_expr = Self::create_physical_expression(input, expr)?;
                    let name = phys_expr
                        .as_any()
                        .downcast_ref::<ColumnExpr>()
                        .ok_or_else(|| Error::InvalidOperation {
                            message: format!(
                                "Failed to downcast physical expression '{}' to ColumnExpr",
                                phys_expr
                            ),
                            location: location!(),
                        })?
                        .name()
                        .to_string();
                    group_by.push((phys_expr, name))
                }

                let aggregate_expressions = Self::create_aggregate_expression(plan)?;

                Ok(Arc::new(AggregateExec::try_new(
                    physical_input,
                    group_by,
                    aggregate_expressions,
                )?))
            }
            Sort(plan) => {
                let physical_input = Self::create_physical_plan(plan.input())?;
                let order_by = plan
                    .expressions()
                    .iter()
                    .map(|expr| Self::create_physical_expression(input, expr))
                    .collect::<Result<Vec<_>>>()?;

                Ok(Arc::new(SortExec::new(physical_input, order_by)))
            }
            Limit(plan) => {
                let physical_input = Self::create_physical_plan(plan.input())?;
                let skip = plan.skip();
                let fetch = plan.fetch();

                Ok(Arc::new(LimitExec::new(physical_input, skip, fetch)))
            }
            Join(plan) => {
                let lhs = Self::create_physical_plan(plan.lhs())?;
                let rhs = Self::create_physical_plan(plan.rhs())?;
                let join_type = plan.join_type();

                let on = plan
                    .on()
                    .iter()
                    .map(|exprs| {
                        let on_left = Self::create_physical_expression(plan.lhs(), &exprs.0)?;
                        let on_right = Self::create_physical_expression(plan.rhs(), &exprs.1)?;
                        Ok((on_left, on_right))
                    })
                    .collect::<Result<JoinOn>>()?;

                let filter = match plan.filter() {
                    Some(_expr) => {
                        // get columns from expression
                        // collect left & right field indices, map over cols get idx from
                        // left/right schema.
                        // create left/right fields from indices and left/right schema
                        // construct filter_schema
                        // eval logical filter_expr -> physical filter expr
                        // build JoinColumnIndices
                        // create JoinFilter
                        todo!()
                    }
                    None => None,
                };

                // TODO: if no 'join-on' condition we should use NestedLoopJoin
                // which has yet to be implemented.
                Ok(Arc::new(HashJoinExec::try_new(
                    lhs, rhs, on, filter, join_type,
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
                let aggr_expr: Arc<dyn AggregateExpr> = match agg.func() {
                    AggregateFunction::Count => Arc::new(CountExpr::new(phys_expr)),
                    AggregateFunction::Sum => make_aggregate_expr!(phys_expr, plan, SumExpr),
                    AggregateFunction::Avg => make_aggregate_expr!(phys_expr, plan, AvgExpr),
                    AggregateFunction::Max => make_aggregate_expr!(phys_expr, plan, MaxExpr),
                    AggregateFunction::Min => make_aggregate_expr!(phys_expr, plan, MinExpr),
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
            Sort(v) => {
                let expression = Self::create_physical_expression(input, v.expression())?;
                let sort_expression = SortExpr::new(expression, v.ascending());
                Ok(Arc::new(sort_expression))
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

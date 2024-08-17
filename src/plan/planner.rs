use std::{collections::HashSet, sync::Arc};

use arrow::datatypes::Schema;
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{
        logical::{aggregate::AggregateFunction, column::Column, expr::Expression},
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
            joins::{
                hash_join::HashJoinExec,
                utils::{JoinColumnIndex, JoinFilter, JoinOn, JoinSide},
            },
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
                    let expr = Self::create_physical_expression(&plan.input().schema(), curr_expr)?;
                    fields.push(field);
                    expression.push(expr);
                }
                let schema = Arc::new(Schema::new(fields));

                Ok(Arc::new(ProjectionExec::new(input, schema, expression)))
            }
            Filter(plan) => {
                let physical_input = Self::create_physical_plan(plan.input())?;
                let predicate =
                    Self::create_physical_expression(&input.schema(), &plan.expressions()[0])?;

                Ok(Arc::new(FilterExec::try_new(physical_input, predicate)?))
            }
            Aggregate(plan) => {
                let physical_input = Self::create_physical_plan(plan.input())?;

                let mut group_by = Vec::with_capacity(plan.group_by().len());
                for expr in plan.group_by().iter() {
                    let phys_expr = Self::create_physical_expression(&input.schema(), expr)?;
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
                    .map(|expr| Self::create_physical_expression(&input.schema(), expr))
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
                        let on_left =
                            Self::create_physical_expression(&plan.lhs().schema(), &exprs.0)?;
                        let on_right =
                            Self::create_physical_expression(&plan.rhs().schema(), &exprs.1)?;
                        Ok((on_left, on_right))
                    })
                    .collect::<Result<JoinOn>>()?;

                let filter = match plan.filter() {
                    Some(expr) => {
                        let mut columns = HashSet::new();
                        Self::collect_columns(expr, &mut columns);

                        let lhs_schema = plan.lhs().schema();
                        let rhs_schema = plan.rhs().schema();

                        let mut filter_fields = Vec::with_capacity(columns.len());
                        let mut filter_indices = Vec::with_capacity(columns.len());
                        for col in &columns {
                            let lhs_fields = lhs_schema.column_with_name(col.name());
                            if let Some((idx, field)) = lhs_fields {
                                filter_fields.push(field.clone());
                                filter_indices.push(JoinColumnIndex::new(idx, JoinSide::Left));
                            }
                            let rhs_fields = rhs_schema.column_with_name(col.name());
                            if let Some((idx, field)) = rhs_fields {
                                filter_fields.push(field.clone());
                                filter_indices.push(JoinColumnIndex::new(idx, JoinSide::Right));
                            }
                        }

                        let filter_schema = Arc::new(Schema::new(filter_fields));
                        let filter_expression =
                            Self::create_physical_expression(&filter_schema, expr)?;
                        let join_filter =
                            JoinFilter::new(filter_schema, filter_expression, filter_indices);

                        Some(join_filter)
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
                let phys_expr =
                    Self::create_physical_expression(&plan.input().schema(), agg.expression())?;
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

    /// Recursively collect all referenced columns from the expression.
    fn collect_columns(expr: &Expression, columns: &mut HashSet<Column>) {
        use Expression::*;

        match expr {
            Column(e) => {
                columns.insert(e.clone());
            }
            Binary(e) => {
                Self::collect_columns(e.lhs(), columns);
                Self::collect_columns(e.rhs(), columns);
            }
            _ => {}
        }
    }

    /// Converts a logical to a physical expression.
    fn create_physical_expression(
        schema: &Schema,
        expr: &Expression,
    ) -> Result<Arc<dyn PhysicalExpression>> {
        use Expression::*;

        match expr {
            Column(v) => {
                let (index, _) =
                    schema
                        .column_with_name(v.name())
                        .ok_or_else(|| Error::InvalidData {
                            message: format!(
                                "Column with name '{}' could not be found in schema",
                                v.name()
                            ),
                            location: location!(),
                        })?;
                Ok(Arc::new(ColumnExpr::new(v.name(), index)))
            }
            Literal(v) => Ok(Arc::new(LiteralExpr::new(v.clone()))),
            Binary(v) => {
                let left = Self::create_physical_expression(schema, v.lhs())?;
                let right = Self::create_physical_expression(schema, v.rhs())?;
                Ok(Arc::new(BinaryExpr::new(left, v.op().clone(), right)))
            }
            Sort(v) => {
                let expression = Self::create_physical_expression(schema, v.expression())?;
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

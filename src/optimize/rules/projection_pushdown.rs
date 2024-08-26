use std::{collections::HashSet, sync::Arc};

use crate::{
    error::Result,
    expression::logical::{column::Column, expr::Expression},
    plan::logical::{
        aggregate::Aggregate, filter::Filter, join::Join, limit::Limit, plan::LogicalPlan,
        projection::Projection, scan::Scan, sort::Sort,
    },
};

use super::{utils::collect_columns, OptimizerRule, RecursionState};

#[derive(Debug, Default)]
pub struct ProjectionPushDownRule;

impl ProjectionPushDownRule {
    pub fn new() -> Self {
        Self {}
    }

    fn push_down(
        plan: &LogicalPlan,
        projected_columns: &mut Vec<String>,
    ) -> Result<Option<LogicalPlan>> {
        let new_plan = match plan {
            LogicalPlan::Projection(projection) => {
                match Self::push_down_impl(projection, projected_columns)? {
                    RecursionState::Stop(opt_plan) => opt_plan,
                    RecursionState::Continue(plan) => Self::push_down(&plan, projected_columns)?,
                }
            }
            LogicalPlan::Sort(sort) => {
                Self::collect_columns_by_name(sort.expressions(), projected_columns);
                let input = Self::push_down(sort.input(), projected_columns)?
                    .unwrap_or(sort.input().clone());
                let new_plan =
                    LogicalPlan::Sort(Sort::new(Arc::new(input), sort.expressions().to_vec()));
                Some(new_plan)
            }
            LogicalPlan::Limit(limit) => {
                let input = Self::push_down(limit.input(), projected_columns)?
                    .unwrap_or(limit.input().clone());
                let new_plan =
                    LogicalPlan::Limit(Limit::new(Arc::new(input), limit.skip(), limit.fetch()));
                Some(new_plan)
            }
            LogicalPlan::Filter(filter) => {
                Self::collect_columns_by_name(filter.expressions(), projected_columns);
                let input = Self::push_down(filter.input(), projected_columns)?
                    .unwrap_or(filter.input().clone());
                let new_plan = LogicalPlan::Filter(Filter::try_new(
                    Arc::new(input),
                    filter.expressions()[0].clone(),
                )?);
                Some(new_plan)
            }
            LogicalPlan::Aggregate(agg) => {
                Self::collect_columns_by_name(agg.group_by(), projected_columns);
                Self::collect_columns_by_name(agg.aggregate_expressions(), projected_columns);
                let input =
                    Self::push_down(agg.input(), projected_columns)?.unwrap_or(agg.input().clone());
                let new_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(input),
                    agg.group_by().to_vec(),
                    agg.aggregate_expressions().to_vec(),
                )?);
                Some(new_plan)
            }
            LogicalPlan::Join(join) => {
                if let Some(join_filter) = join.filter() {
                    Self::collect_columns_by_name(&[join_filter.clone()], projected_columns);
                }
                for (lhs_on, rhs_on) in join.on() {
                    Self::collect_columns_by_name(&[lhs_on.clone()], projected_columns);
                    Self::collect_columns_by_name(&[rhs_on.clone()], projected_columns);
                }
                let lhs_input =
                    Self::push_down(join.lhs(), projected_columns)?.unwrap_or(join.lhs().clone());
                let rhs_input =
                    Self::push_down(join.rhs(), projected_columns)?.unwrap_or(join.rhs().clone());
                let new_plan = LogicalPlan::Join(Join::new(
                    Arc::new(lhs_input),
                    Arc::new(rhs_input),
                    join.on().to_vec(),
                    join.join_type(),
                    join.filter().cloned(),
                ));
                Some(new_plan)
            }
            _ => None,
        };

        Ok(new_plan)
    }

    fn push_down_impl(
        projection: &Projection,
        columns: &mut Vec<String>,
    ) -> Result<RecursionState> {
        Self::collect_columns_by_name(projection.expressions(), columns);

        let child_plan = projection.input();

        let new_plan = match child_plan {
            LogicalPlan::Scan(scan) => {
                let schema = scan.schema();
                let fields = schema.fields();

                let to_push = fields
                    .iter()
                    .map(|f| f.name().to_string())
                    .filter(|n| columns.contains(n))
                    .collect::<Vec<_>>();

                let projection = if to_push.is_empty() {
                    None
                } else {
                    Some(to_push)
                };

                let new_plan = LogicalPlan::Scan(Scan::new(
                    scan.path(),
                    scan.source(),
                    projection,
                    scan.expressions().to_vec(),
                ));

                RecursionState::Stop(Some(new_plan))
            }
            LogicalPlan::Projection(proj) => {
                Self::collect_columns_by_name(proj.expressions(), columns);

                let new_projection = columns
                    .iter()
                    .map(|n| Expression::Column(Column::new(n)))
                    .collect::<Vec<_>>();

                let new_plan = LogicalPlan::Projection(Projection::new(
                    Arc::new(proj.input().clone()),
                    new_projection,
                ));
                RecursionState::Continue(new_plan)
            }
            LogicalPlan::Sort(sort) => {
                Self::collect_columns_by_name(sort.expressions(), columns);
                let new_projection = LogicalPlan::Projection(Projection::new(
                    Arc::new(sort.input().clone()),
                    projection.expressions().to_vec(),
                ));
                let new_plan = LogicalPlan::Sort(Sort::new(
                    Arc::new(new_projection),
                    sort.expressions().to_vec(),
                ));
                RecursionState::Continue(new_plan)
            }
            LogicalPlan::Limit(limit) => {
                let new_projection = LogicalPlan::Projection(Projection::new(
                    Arc::new(limit.input().clone()),
                    projection.expressions().to_vec(),
                ));
                let new_plan = LogicalPlan::Limit(Limit::new(
                    Arc::new(new_projection),
                    limit.skip(),
                    limit.fetch(),
                ));
                RecursionState::Continue(new_plan)
            }
            LogicalPlan::Filter(filter) => {
                Self::collect_columns_by_name(filter.expressions(), columns);
                let new_projection = LogicalPlan::Projection(Projection::new(
                    Arc::new(filter.input().clone()),
                    projection.expressions().to_vec(),
                ));
                let new_plan = LogicalPlan::Filter(Filter::try_new(
                    Arc::new(new_projection),
                    filter.expressions()[0].clone(),
                )?);
                RecursionState::Continue(new_plan)
            }
            LogicalPlan::Aggregate(agg) => {
                Self::collect_columns_by_name(agg.group_by(), columns);
                Self::collect_columns_by_name(agg.aggregate_expressions(), columns);
                let new_projection = LogicalPlan::Projection(Projection::new(
                    Arc::new(agg.input().clone()),
                    projection.expressions().to_vec(),
                ));
                let new_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(new_projection),
                    agg.group_by().to_vec(),
                    agg.aggregate_expressions().to_vec(),
                )?);
                RecursionState::Continue(new_plan)
            }
            LogicalPlan::Join(join) => {
                if let Some(join_filter) = join.filter() {
                    Self::collect_columns_by_name(&[join_filter.clone()], columns);
                }
                for (lhs_on, rhs_on) in join.on() {
                    Self::collect_columns_by_name(&[lhs_on.clone()], columns);
                    Self::collect_columns_by_name(&[rhs_on.clone()], columns);
                }
                let lhs_new_projection = LogicalPlan::Projection(Projection::new(
                    Arc::new(join.lhs().clone()),
                    projection.expressions().to_vec(),
                ));
                let rhs_new_projection = LogicalPlan::Projection(Projection::new(
                    Arc::new(join.rhs().clone()),
                    projection.expressions().to_vec(),
                ));
                let new_plan = LogicalPlan::Join(Join::new(
                    Arc::new(lhs_new_projection),
                    Arc::new(rhs_new_projection),
                    join.on().to_vec(),
                    join.join_type(),
                    join.filter().cloned(),
                ));
                RecursionState::Continue(new_plan)
            }
        };

        Ok(new_plan)
    }

    fn collect_columns_by_name(exprs: &[Expression], columns: &mut Vec<String>) {
        let mut referenced_columns = HashSet::new();
        for expr in exprs {
            collect_columns(expr, &mut referenced_columns);
        }
        let columns_by_name = referenced_columns
            .into_iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>();
        columns.extend(columns_by_name);
    }
}

impl OptimizerRule for ProjectionPushDownRule {
    fn name(&self) -> &str {
        "ProjectionPushDownRule"
    }

    fn try_optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let mut projected_columns = vec![];
        Self::push_down(plan, &mut projected_columns)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        expression::logical::{
            expr::Expression,
            expr_fn::{col, lit, sum},
        },
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        optimize::rules::{projection_pushdown::ProjectionPushDownRule, OptimizerRule},
        plan::logical::{
            aggregate::Aggregate,
            filter::Filter,
            join::{Join, JoinType},
            limit::Limit,
            plan::LogicalPlan,
            projection::Projection,
            scan::Scan,
            sort::Sort,
        },
    };

    fn create_scan() -> Arc<LogicalPlan> {
        let path = "testdata/csv/simple.csv";
        let source = Arc::new(CsvDataSource::try_new(path, CsvReadOptions::new()).unwrap());
        Arc::new(LogicalPlan::Scan(Scan::new(path, source, None, vec![])))
    }

    fn create_projection(input: Arc<LogicalPlan>, projection: Vec<Expression>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Projection(Projection::new(input, projection)))
    }

    #[test]
    fn test_projection_pushdown_with_join() {
        let lhs = create_scan();
        let rhs = create_scan();
        let input = Arc::new(LogicalPlan::Join(Join::new(
            lhs,
            rhs,
            vec![(col("c1"), col("c1"))],
            JoinType::Left,
            None,
        )));

        let input = create_projection(input, vec![col("c2")]);

        let rule = ProjectionPushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{result}"),
            "Join: [type: LEFT, on: [(Column(Column { name: \"c1\" }), Column(Column { name: \"c1\" }))]]\n\tScan: testdata/csv/simple.csv; projection=[\"c1\", \"c2\"]; filter=[[]]\n\tScan: testdata/csv/simple.csv; projection=[\"c1\", \"c2\"]; filter=[[]]\n"
        );
    }

    #[test]
    fn test_projection_pushdown_with_aggregate() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Aggregate(
            Aggregate::try_new(input, vec![col("c1"), col("c2")], vec![sum(col("c2"))]).unwrap(),
        ));
        let input = create_projection(input, vec![col("c3")]);

        let rule = ProjectionPushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{result}"),
            "Aggregate: groupBy:[c1, c2]; aggrExprs:[SUM(c2)]\n\tScan: testdata/csv/simple.csv; projection=[\"c1\", \"c2\", \"c3\"]; filter=[[]]\n"
        );
    }

    #[test]
    fn test_projection_pushdown_with_filter() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Filter(
            Filter::try_new(input, col("c2").lt(lit(4i64))).unwrap(),
        ));
        let input = create_projection(input, vec![col("c1")]);

        let rule = ProjectionPushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{result}"),
            "Filter: [c2 < 4]\n\tScan: testdata/csv/simple.csv; projection=[\"c1\", \"c2\"]; filter=[[]]\n"
        );
    }

    #[test]
    fn test_projection_pushdown_with_limit() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Limit(Limit::new(input, 0, Some(1))));
        let input = create_projection(input, vec![col("c1")]);

        let rule = ProjectionPushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{result}"),
            "Limit: [skip: 0, fetch:Some(1)]\n\tScan: testdata/csv/simple.csv; projection=[\"c1\"]; filter=[[]]\n"
        );
    }

    #[test]
    fn test_projection_pushdown_with_sort() {
        let input = create_scan();
        let input = create_projection(input, vec![col("c1")]);
        let input = Arc::new(LogicalPlan::Sort(Sort::new(input, vec![col("c2")])));
        let input = create_projection(input, vec![col("c3")]);

        let rule = ProjectionPushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{result}"),
            "Sort: [c2]\n\tScan: testdata/csv/simple.csv; projection=[\"c1\", \"c2\", \"c3\"]; filter=[[]]\n"
        );
    }

    #[test]
    fn test_projection_pushdown_nested_projection() {
        let input = create_scan();
        let projection = vec![col("c1")];
        let input = create_projection(input, projection);
        let projection = vec![col("c2")];
        let input = create_projection(input, projection);

        let rule = ProjectionPushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{result}"),
            "Scan: testdata/csv/simple.csv; projection=[\"c1\", \"c2\"]; filter=[[]]\n"
        );
    }
}

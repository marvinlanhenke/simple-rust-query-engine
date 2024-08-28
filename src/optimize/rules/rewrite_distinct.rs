use std::sync::Arc;

use itertools::Itertools;

use crate::{
    error::Result,
    expression::logical::{column::Column, expr::Expression},
    plan::logical::{
        aggregate::Aggregate, filter::Filter, join::Join, limit::Limit, plan::LogicalPlan,
        projection::Projection, sort::Sort,
    },
};

use super::OptimizerRule;

#[derive(Debug, Default)]
pub struct RewriteDistinctRule;

impl RewriteDistinctRule {
    pub fn new() -> Self {
        Self {}
    }

    fn rewrite(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let new_plan = match plan {
            LogicalPlan::Distinct(dist) => {
                let input = dist.input();
                let schema = input.schema();
                let group_by = schema
                    .fields()
                    .iter()
                    .map(|f| Expression::Column(Column::new(f.name())))
                    .unique()
                    .collect::<Vec<_>>();

                let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(input.clone()),
                    group_by,
                    vec![],
                )?);

                Some(aggregate)
            }
            LogicalPlan::Projection(proj) => {
                let input = Self::rewrite(proj.input())?.unwrap_or(proj.input().clone());
                let new_plan = LogicalPlan::Projection(Projection::new(
                    Arc::new(input),
                    proj.expressions().to_vec(),
                ));
                Some(new_plan)
            }
            LogicalPlan::Sort(sort) => {
                let input = Self::rewrite(sort.input())?.unwrap_or(sort.input().clone());
                let new_plan =
                    LogicalPlan::Sort(Sort::new(Arc::new(input), sort.expressions().to_vec()));
                Some(new_plan)
            }
            LogicalPlan::Limit(limit) => {
                let input = Self::rewrite(limit.input())?.unwrap_or(limit.input().clone());
                let new_plan =
                    LogicalPlan::Limit(Limit::new(Arc::new(input), limit.skip(), limit.fetch()));
                Some(new_plan)
            }
            LogicalPlan::Aggregate(agg) => {
                let input = Self::rewrite(agg.input())?.unwrap_or(agg.input().clone());
                let new_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(input),
                    agg.group_by().to_vec(),
                    agg.aggregate_expressions().to_vec(),
                )?);
                Some(new_plan)
            }
            LogicalPlan::Filter(filter) => {
                let input = Self::rewrite(filter.input())?.unwrap_or(filter.input().clone());
                let new_plan = LogicalPlan::Filter(Filter::try_new(
                    Arc::new(input),
                    filter.expressions()[0].clone(),
                )?);
                Some(new_plan)
            }
            LogicalPlan::Join(join) => {
                let lhs = Self::rewrite(join.lhs())?.unwrap_or(join.lhs().clone());
                let rhs = Self::rewrite(join.rhs())?.unwrap_or(join.rhs().clone());
                let new_plan = LogicalPlan::Join(Join::try_new(
                    Arc::new(lhs),
                    Arc::new(rhs),
                    join.on().to_vec(),
                    join.join_type(),
                    join.filter().cloned(),
                )?);
                Some(new_plan)
            }
            _ => Some(plan.clone()),
        };

        Ok(new_plan)
    }
}

impl OptimizerRule for RewriteDistinctRule {
    fn name(&self) -> &str {
        "RewriteDistinctRule"
    }

    fn try_optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Self::rewrite(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        expression::logical::expr_fn::{col, lit},
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        optimize::rules::OptimizerRule,
        plan::logical::{
            distinct::Distinct, filter::Filter, plan::LogicalPlan, projection::Projection,
            scan::Scan,
        },
    };

    use super::RewriteDistinctRule;

    fn create_scan() -> Arc<LogicalPlan> {
        let path = "testdata/csv/simple.csv";
        let source = Arc::new(CsvDataSource::try_new(path, CsvReadOptions::new()).unwrap());
        Arc::new(LogicalPlan::Scan(Scan::new(path, source, None, vec![])))
    }

    #[test]
    fn test_rewrite_distinct_nested() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Distinct(Distinct::new(input)));
        let input = Arc::new(LogicalPlan::Filter(
            Filter::try_new(input, col("c2").eq(lit(4i64))).unwrap(),
        ));
        let input = Arc::new(LogicalPlan::Projection(Projection::new(
            input,
            vec![col("c1")],
        )));

        let rule = RewriteDistinctRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
        format!("{result}"),
        "Projection: [c1]\n\tFilter: [c2 = 4]\n\t\tAggregate: groupBy:[c1, c2, c3]; aggrExprs:[]\n\t\t\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }
}

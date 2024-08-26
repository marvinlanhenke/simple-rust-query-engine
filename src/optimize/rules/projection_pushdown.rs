use std::{collections::HashSet, sync::Arc};

use crate::{
    error::Result,
    expression::logical::expr::Expression,
    plan::logical::{plan::LogicalPlan, projection::Projection, scan::Scan},
};

use super::{utils::collect_columns, OptimizerRule, RecursionState};

#[derive(Debug, Default)]
pub struct ProjectionPushDownRule;

impl ProjectionPushDownRule {
    pub fn new() -> Self {
        Self {}
    }

    fn push_down(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let new_plan = match plan {
            LogicalPlan::Projection(projection) => match Self::push_down_impl(projection)? {
                RecursionState::Stop(opt_plan) => opt_plan,
                RecursionState::Continue(plan) => Self::push_down(&plan)?,
            },
            _ => None,
        };

        Ok(new_plan)
    }

    fn push_down_impl(projection: &Projection) -> Result<RecursionState> {
        let child_plan = projection.input();

        let new_plan = match child_plan {
            LogicalPlan::Scan(scan) => {
                let schema = scan.schema();
                let fields = schema.fields();

                let mut columns = HashSet::new();
                for expr in projection.expressions() {
                    collect_columns(expr, &mut columns);
                }
                let projected_columns_by_name = columns
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect::<Vec<_>>();

                let to_push = fields
                    .iter()
                    .map(|f| f.name().to_string())
                    .filter(|n| projected_columns_by_name.contains(n))
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
                let mut columns = HashSet::new();
                for expr in proj.expressions() {
                    collect_columns(expr, &mut columns);
                }
                let child_projection = columns
                    .into_iter()
                    .map(Expression::Column)
                    .collect::<Vec<_>>();
                let mut new_projection = projection.expressions().to_vec();
                new_projection.extend(child_projection);

                let new_plan = LogicalPlan::Projection(Projection::new(
                    Arc::new(proj.input().clone()),
                    new_projection,
                ));
                RecursionState::Continue(new_plan)
            }
            _ => RecursionState::Stop(None),
        };

        Ok(new_plan)
    }
}

impl OptimizerRule for ProjectionPushDownRule {
    fn name(&self) -> &str {
        "ProjectionPushDownRule"
    }

    fn try_optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Self::push_down(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        expression::logical::{expr::Expression, expr_fn::col},
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        optimize::rules::{projection_pushdown::ProjectionPushDownRule, OptimizerRule},
        plan::logical::{plan::LogicalPlan, projection::Projection, scan::Scan},
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

use std::sync::Arc;

use crate::{
    error::Result,
    expression::{logical::expr::Expression, operator::Operator},
    plan::logical::{filter::Filter, plan::LogicalPlan, projection::Projection},
};

use super::OptimizerRule;

#[derive(Debug, Default)]
pub struct PredicatePushDownRuleNew;

impl PredicatePushDownRuleNew {
    pub fn new() -> Self {
        Self {}
    }

    fn push_down(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let new_plan = match plan {
            LogicalPlan::Filter(filter) => match Self::push_down_impl(filter)? {
                Some(plan) => Self::push_down(&plan)?,
                None => Some(plan.clone()),
            },
            LogicalPlan::Projection(projection) => {
                let input =
                    Self::push_down(projection.input())?.unwrap_or(projection.input().clone());
                let new_plan = LogicalPlan::Projection(Projection::new(
                    Arc::new(input),
                    projection.expressions().to_vec(),
                ));
                Some(new_plan)
            }
            _ => None,
        };

        Ok(new_plan)
    }

    fn push_down_impl(filter: &Filter) -> Result<Option<LogicalPlan>> {
        let child_plan = filter.input();

        let new_plan = match child_plan {
            LogicalPlan::Projection(projection) => {
                let predicates = Self::split_conjunction(&filter.expressions()[0], vec![])
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let projection_expression = projection.expressions().to_vec();

                match Self::conjunction(predicates) {
                    Some(predicates) => {
                        let input = projection.input().clone();
                        let new_filter =
                            LogicalPlan::Filter(Filter::try_new(Arc::new(input), predicates)?);
                        let new_plan = LogicalPlan::Projection(Projection::new(
                            Arc::new(new_filter),
                            projection_expression,
                        ));
                        Some(new_plan)
                    }
                    None => None,
                }
            }
            _ => None,
        };
        Ok(new_plan)
    }

    /// Splits a conjunction expression into its separate parts.
    /// i.e. `c1 > 5 AND c2 > 4` becomes `[c1 > 5, c2 > 4]`.
    fn split_conjunction<'a>(
        expr: &'a Expression,
        mut exprs: Vec<&'a Expression>,
    ) -> Vec<&'a Expression> {
        match expr {
            Expression::Binary(e) if e.op() == &Operator::And => {
                let lhs = e.lhs();
                let rhs = e.rhs();
                let exprs = Self::split_conjunction(lhs, exprs);
                let exprs = Self::split_conjunction(rhs, exprs);
                exprs
            }
            other => {
                exprs.push(other);
                exprs
            }
        }
    }

    /// Combines a list of expresssions into a single conjunction predicate.
    fn conjunction(exprs: Vec<Expression>) -> Option<Expression> {
        exprs.into_iter().reduce(|acc, expr| acc.and(expr))
    }
}

impl OptimizerRule for PredicatePushDownRuleNew {
    fn name(&self) -> &str {
        "PredicatePushDown"
    }

    fn try_optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Self::push_down(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        expression::logical::{
            expr::Expression,
            expr_fn::{col, lit},
        },
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        optimize::rules::{predicate_pushdown_new::PredicatePushDownRuleNew, OptimizerRule},
        plan::logical::{filter::Filter, plan::LogicalPlan, projection::Projection, scan::Scan},
    };

    fn create_scan() -> Arc<LogicalPlan> {
        let path = "testdata/csv/simple.csv";
        let source = Arc::new(CsvDataSource::try_new(path, CsvReadOptions::new()).unwrap());
        Arc::new(LogicalPlan::Scan(Scan::new(path, source, None, vec![])))
    }

    fn create_filter(input: Arc<LogicalPlan>, predicate: Expression) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Filter(
            Filter::try_new(input, predicate).unwrap(),
        ))
    }

    #[test]
    fn test_stub() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Projection(Projection::new(
            input,
            vec![col("c2")],
        )));
        let predicate = col("c2").eq(lit(5i64));
        let input = create_filter(input, predicate);
        let input = Arc::new(LogicalPlan::Projection(Projection::new(
            input,
            vec![col("c1")],
        )));

        println!("before >> {input}");

        let rule = PredicatePushDownRuleNew::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        println!("{result}");
    }
}

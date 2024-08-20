use std::sync::Arc;

use itertools::Itertools;

use crate::{
    error::Result,
    expression::{logical::expr::Expression, operator::Operator},
    io::PredicatePushDownSupport,
    plan::logical::{filter::Filter, plan::LogicalPlan, scan::Scan},
};

use super::OptimizerRule;

#[derive(Debug, Default)]
pub struct PredicatePushDownRule;

impl PredicatePushDownRule {
    pub fn new() -> Self {
        Self {}
    }

    fn push_down(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let filter = match plan {
            LogicalPlan::Filter(filter) => filter,
            _ => return Ok(None),
        };

        let child_plan = filter.input();

        let new_plan = match child_plan {
            LogicalPlan::Scan(plan) => {
                let predicates = Self::split_conjunction(&filter.expressions()[0], vec![]);

                let supported = plan
                    .source()
                    .can_pushdown_predicates(predicates.as_slice())?;

                let zip = predicates.iter().zip(supported);
                let to_pushdown = zip.clone().filter_map(|(e, s)| {
                    if s == PredicatePushDownSupport::Unsupported {
                        return None;
                    }
                    Some(*e)
                });
                let new_scan_predicates = plan
                    .expressions()
                    .iter()
                    .chain(to_pushdown)
                    .unique()
                    .cloned()
                    .collect::<Vec<_>>();
                let new_plan = LogicalPlan::Scan(Scan::new(
                    plan.path(),
                    plan.source(),
                    plan.projection().cloned(),
                    new_scan_predicates,
                ));
                let new_predicate = zip
                    .clone()
                    .filter_map(|(e, s)| {
                        if s == PredicatePushDownSupport::Exact {
                            return None;
                        }
                        Some((*e).clone())
                    })
                    .collect::<Vec<_>>();

                match Self::conjunction(new_predicate) {
                    Some(predicate) => Some(LogicalPlan::Filter(Filter::try_new(
                        Arc::new(new_plan),
                        predicate,
                    )?)),
                    None => Some(new_plan),
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

impl OptimizerRule for PredicatePushDownRule {
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
        expression::logical::expr_fn::{col, lit},
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        optimize::rules::OptimizerRule,
        plan::logical::{filter::Filter, plan::LogicalPlan, scan::Scan},
    };

    use super::PredicatePushDownRule;

    fn create_scan() -> Arc<LogicalPlan> {
        let path = "testdata/csv/simple.csv";
        let source = Arc::new(CsvDataSource::try_new(path, CsvReadOptions::new()).unwrap());
        Arc::new(LogicalPlan::Scan(Scan::new(path, source, None, vec![])))
    }

    #[test]
    fn test_predicate_push_down_with_expression() {
        let input = create_scan();
        let predicate = col("c2").eq(lit(5i64));
        let filter = LogicalPlan::Filter(Filter::try_new(input, predicate).unwrap());

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&filter).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Filter: [c2 = 5]\n\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }
}

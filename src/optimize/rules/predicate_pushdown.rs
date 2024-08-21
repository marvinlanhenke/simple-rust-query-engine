use std::{collections::HashSet, sync::Arc};

use itertools::Itertools;
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{logical::expr::Expression, operator::Operator},
    io::PredicatePushDownSupport,
    plan::logical::{filter::Filter, plan::LogicalPlan, projection::Projection, scan::Scan},
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
            LogicalPlan::Scan(scan) => {
                let predicates = Self::split_conjunction(&filter.expressions()[0], vec![]);

                let supported = scan
                    .source()
                    .can_pushdown_predicates(predicates.as_slice())?;

                let zip = predicates.iter().zip(supported);
                let to_pushdown = zip.clone().filter_map(|(e, s)| {
                    if s == PredicatePushDownSupport::Unsupported {
                        return None;
                    }
                    Some(*e)
                });
                let new_scan_predicates = scan
                    .expressions()
                    .iter()
                    .chain(to_pushdown)
                    .unique()
                    .cloned()
                    .collect::<Vec<_>>();
                let new_plan = LogicalPlan::Scan(Scan::new(
                    scan.path(),
                    scan.source(),
                    scan.projection().cloned(),
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
            LogicalPlan::Filter(child_filter) => {
                let predicates = Self::split_conjunction(&filter.expressions()[0], vec![]);
                let set: HashSet<&&Expression> = predicates.iter().collect();

                let unique_predicates = predicates
                    .iter()
                    .chain(
                        Self::split_conjunction(&child_filter.expressions()[0], vec![])
                            .iter()
                            .filter(|expr| !set.contains(expr)),
                    )
                    .map(|expr| (*expr).clone())
                    .collect::<Vec<_>>();
                let new_predicate =
                    Self::conjunction(unique_predicates).ok_or_else(|| Error::InvalidData {
                        message: "At least one filter expression should exist".to_string(),
                        location: location!(),
                    })?;
                let new_filter = LogicalPlan::Filter(Filter::try_new(
                    Arc::new(child_filter.input().clone()),
                    new_predicate,
                )?);

                Some(Self::push_down(&new_filter)?.unwrap_or(new_filter))
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
        plan::logical::{filter::Filter, plan::LogicalPlan, projection::Projection, scan::Scan},
    };

    use super::PredicatePushDownRule;

    fn create_scan() -> Arc<LogicalPlan> {
        let path = "testdata/csv/simple.csv";
        let source = Arc::new(CsvDataSource::try_new(path, CsvReadOptions::new()).unwrap());
        Arc::new(LogicalPlan::Scan(Scan::new(path, source, None, vec![])))
    }

    fn create_filter(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        let predicate = col("c2").eq(lit(5i64));
        Arc::new(LogicalPlan::Filter(
            Filter::try_new(input, predicate).unwrap(),
        ))
    }

    fn create_filter2(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        let predicate = col("c2").lt(lit(5i64)).and(col("c2").gt(lit(10i64)));
        Arc::new(LogicalPlan::Filter(
            Filter::try_new(input, predicate).unwrap(),
        ))
    }

    #[test]
    fn test_predicate_push_down_with_nested_filters_duplicate_expression() {
        let input = create_scan();
        let input = create_filter(input);
        let filter = create_filter(input);

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&filter).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Filter: [c2 = 5]\n\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_push_down_with_nested_filters() {
        let input = create_scan();
        let input = create_filter(input);
        let filter = create_filter2(input);

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&filter).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Filter: [c2 < 5 AND c2 > 10 AND c2 = 5]\n\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_push_down_projection_with_expressions() {
        let input = create_scan();
        let input = LogicalPlan::Projection(Projection::new(input, vec![col("c1")]));
        let filter = create_filter(Arc::new(input));

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&filter).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Projection: [c1]\n\tFilter: [c2 = 5]\n\t\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_push_down_scan_with_expressions() {
        let input = create_scan();
        let filter = create_filter(input);

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&filter).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Filter: [c2 = 5]\n\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }
}

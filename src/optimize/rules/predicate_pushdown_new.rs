use std::sync::Arc;

use itertools::Itertools;

use crate::{
    error::Result,
    expression::{logical::expr::Expression, operator::Operator},
    io::PredicatePushDownSupport,
    plan::logical::{filter::Filter, plan::LogicalPlan, projection::Projection, scan::Scan},
};

use super::OptimizerRule;

enum RecursionState {
    Continue(LogicalPlan),
    Stop(Option<LogicalPlan>),
}

#[derive(Debug, Default)]
pub struct PredicatePushDownRuleNew;

impl PredicatePushDownRuleNew {
    pub fn new() -> Self {
        Self {}
    }

    fn push_down(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let new_plan = match plan {
            LogicalPlan::Filter(filter) => match Self::push_down_impl(filter)? {
                RecursionState::Stop(opt_plan) => opt_plan,
                RecursionState::Continue(plan) => Self::push_down(&plan)?,
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

    fn push_down_impl(filter: &Filter) -> Result<RecursionState> {
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
                let new_scan = LogicalPlan::Scan(Scan::new(
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
                    Some(predicate) => {
                        let new_filter =
                            LogicalPlan::Filter(Filter::try_new(Arc::new(new_scan), predicate)?);
                        RecursionState::Stop(Some(new_filter))
                    }
                    None => RecursionState::Stop(Some(new_scan)),
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
                        let new_projection = LogicalPlan::Projection(Projection::new(
                            Arc::new(new_filter),
                            projection_expression,
                        ));
                        RecursionState::Continue(new_projection)
                    }
                    None => RecursionState::Stop(None),
                }
            }
            _ => RecursionState::Stop(None),
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

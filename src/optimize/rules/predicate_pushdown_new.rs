use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use itertools::Itertools;
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{
        logical::{column::Column, expr::Expression},
        operator::Operator,
    },
    io::PredicatePushDownSupport,
    plan::logical::{
        aggregate::Aggregate, filter::Filter, plan::LogicalPlan, projection::Projection,
        scan::Scan, sort::Sort,
    },
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
            LogicalPlan::Sort(sort) => {
                let input = Self::push_down(sort.input())?.unwrap_or(sort.input().clone());
                let new_plan =
                    LogicalPlan::Sort(Sort::new(Arc::new(input), sort.expressions().to_vec()));
                Some(new_plan)
            }
            LogicalPlan::Aggregate(agg) => {
                let input = Self::push_down(agg.input())?.unwrap_or(agg.input().clone());
                let new_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(input),
                    agg.group_by().to_vec(),
                    agg.aggregate_expressions().to_vec(),
                )?);
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
                RecursionState::Continue(new_filter)
            }
            LogicalPlan::Sort(sort) => {
                let new_filter = LogicalPlan::Filter(Filter::try_new(
                    Arc::new(sort.input().clone()),
                    filter.expressions()[0].clone(),
                )?);
                let new_plan =
                    LogicalPlan::Sort(Sort::new(Arc::new(new_filter), sort.expressions().to_vec()));
                RecursionState::Continue(new_plan)
            }
            LogicalPlan::Aggregate(agg) => {
                let group_by_cols = agg
                    .group_by()
                    .iter()
                    .map(|expr| Ok(Column::new(expr.display_name()?)))
                    .collect::<Result<HashSet<_>>>()?;
                let predicates = Self::split_conjunction(&filter.expressions()[0], vec![])
                    .iter()
                    .map(|expr| (*expr).clone())
                    .collect::<Vec<_>>();

                let mut replace_map = HashMap::new();
                for expr in agg.group_by() {
                    replace_map.insert(expr.display_name()?, expr.clone());
                }

                let mut to_push = vec![];
                let mut to_keep = vec![];
                for expr in predicates {
                    let mut columns = HashSet::new();
                    Self::collect_columns(&expr, &mut columns);
                    if columns.iter().all(|col| group_by_cols.contains(col)) {
                        to_push.push(expr)
                    } else {
                        to_keep.push(expr)
                    }
                }

                let to_push_replaced = to_push
                    .into_iter()
                    .map(|expr| {
                        if let Expression::Column(col) = &expr {
                            match replace_map.get(col.name()) {
                                Some(new_col_expr) => new_col_expr.clone(),
                                None => expr,
                            }
                        } else {
                            expr
                        }
                    })
                    .collect::<Vec<_>>();
                // Create a new `Filter` with
                // the child plan of the aggregate plan and push it down
                let new_plan = match Self::conjunction(to_push_replaced) {
                    Some(predicate) => {
                        let new_filter = LogicalPlan::Filter(Filter::try_new(
                            Arc::new(agg.input().clone()),
                            predicate,
                        )?);
                        LogicalPlan::Aggregate(Aggregate::try_new(
                            Arc::new(new_filter),
                            agg.group_by().to_vec(),
                            agg.aggregate_expressions().to_vec(),
                        )?)
                    }
                    None => child_plan.clone(),
                };
                // Wrap 'new' aggregate plan with `Filter`
                // for the remaining filters that cannot be pushed
                match Self::conjunction(to_keep) {
                    Some(predicate) => {
                        let new_filter =
                            LogicalPlan::Filter(Filter::try_new(Arc::new(new_plan), predicate)?);
                        RecursionState::Stop(Some(new_filter))
                    }
                    None => RecursionState::Stop(Some(new_plan)),
                }
            }
            _ => RecursionState::Stop(None),
        };

        Ok(new_plan)
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
            expr_fn::{col, lit, sort, sum},
        },
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        optimize::rules::{predicate_pushdown_new::PredicatePushDownRuleNew, OptimizerRule},
        plan::logical::{
            aggregate::Aggregate, filter::Filter, plan::LogicalPlan, projection::Projection,
            scan::Scan, sort::Sort,
        },
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

    #[test]
    fn test_predicate_push_down_aggregate() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Aggregate(
            Aggregate::try_new(input, vec![col("c2")], vec![sum(col("c3"))]).unwrap(),
        ));
        let predicate = col("c2").gt(lit(5i64));
        let input = create_filter(input, predicate);

        let rule = PredicatePushDownRuleNew::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Aggregate: groupBy:[c2]; aggrExprs:[SUM(c3)]\n\tFilter: [c2 > 5]\n\t\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_push_down_sort() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Sort(Sort::new(
            input,
            vec![sort(col("c1"), true)],
        )));
        let predicate = col("c2").eq(lit(5i64));
        let input = create_filter(input, predicate);

        let rule = PredicatePushDownRuleNew::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Sort: [Sort[c1]]\n\tFilter: [c2 = 5]\n\t\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_push_down_with_nested_filters() {
        let input = create_scan();
        let predicate = col("c2").eq(lit(5i64));
        let input = create_filter(input, predicate);
        let predicate = col("c2").lt(lit(5i64)).and(col("c2").gt(lit(10i64)));
        let input = create_filter(input, predicate);

        let rule = PredicatePushDownRuleNew::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Filter: [c2 < 5 AND c2 > 10 AND c2 = 5]\n\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_push_down_projection_with_expressions() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Projection(Projection::new(
            input,
            vec![col("c1")],
        )));
        let predicate = col("c2").eq(lit(5i64));
        let input = create_filter(input, predicate);

        let rule = PredicatePushDownRuleNew::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Projection: [c1]\n\tFilter: [c2 = 5]\n\t\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_push_down_scan_with_expressions() {
        let input = create_scan();
        let predicate = col("c2").eq(lit(5i64));
        let input = create_filter(input, predicate);

        let rule = PredicatePushDownRuleNew::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Filter: [c2 = 5]\n\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }
}

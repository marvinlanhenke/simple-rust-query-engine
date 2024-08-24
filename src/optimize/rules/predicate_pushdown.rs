use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_schema::Schema;
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
        aggregate::Aggregate,
        filter::Filter,
        join::{Join, JoinType},
        plan::LogicalPlan,
        projection::Projection,
        scan::Scan,
        sort::Sort,
    },
};

use super::OptimizerRule;

enum RecursionState {
    Continue(LogicalPlan),
    Stop(Option<LogicalPlan>),
}

#[derive(Debug, Default)]
pub struct PredicatePushDownRule;

impl PredicatePushDownRule {
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
            LogicalPlan::Join(join) => {
                let lhs = Self::push_down(join.lhs())?.unwrap_or(join.lhs().clone());
                let rhs = Self::push_down(join.rhs())?.unwrap_or(join.rhs().clone());
                let new_plan = LogicalPlan::Join(Join::new(
                    Arc::new(lhs),
                    Arc::new(rhs),
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
                    None => RecursionState::Continue(new_plan),
                }
            }
            LogicalPlan::Join(join) => {
                Self::push_down_join(join.clone(), Some(&filter.expressions()[0]))?
            }
            _ => RecursionState::Stop(None),
        };

        Ok(new_plan)
    }

    fn push_down_join(join: Join, parent_predicate: Option<&Expression>) -> Result<RecursionState> {
        let predicates = parent_predicate
            .map_or_else(Vec::new, |expr| Self::split_conjunction(expr, vec![]))
            .iter()
            .map(|expr| (*expr).clone())
            .collect::<Vec<_>>();
        let on_predicates = join
            .filter()
            .map_or_else(Vec::new, |expr| Self::split_conjunction(expr, vec![]))
            .iter()
            .map(|expr| (*expr).clone())
            .collect::<Vec<_>>();
        let inferred_join_predicates =
            Self::infer_join_predicates(&join, &predicates, &on_predicates);

        if predicates.is_empty() && on_predicates.is_empty() && inferred_join_predicates.is_empty()
        {
            return Ok(RecursionState::Continue(LogicalPlan::Join(join.clone())));
        }

        let is_inner_join = join.join_type() == JoinType::Inner;
        let (lhs_preserved, rhs_preserved) = Self::preserved_join_side(join.join_type(), false);
        let lhs_schema = join.lhs().schema();
        let rhs_schema = join.rhs().schema();
        let mut lhs_to_push = vec![];
        let mut rhs_to_push = vec![];
        let mut keep_predicates = vec![];
        let mut join_conditions = vec![];
        for predicate in predicates {
            if lhs_preserved && Self::can_pushdown_predicate(&predicate, &lhs_schema) {
                lhs_to_push.push(predicate);
            } else if rhs_preserved && Self::can_pushdown_predicate(&predicate, &rhs_schema) {
                rhs_to_push.push(predicate);
            } else if is_inner_join {
                join_conditions.push(predicate);
            } else {
                keep_predicates.push(predicate);
            }
        }

        for predicate in inferred_join_predicates {
            if lhs_preserved && Self::can_pushdown_predicate(&predicate, &lhs_schema) {
                lhs_to_push.push(predicate);
            } else if rhs_preserved && Self::can_pushdown_predicate(&predicate, &rhs_schema) {
                rhs_to_push.push(predicate);
            }
        }

        if !on_predicates.is_empty() {
            let (lhs_on_preserved, rhs_on_preserved) =
                Self::preserved_join_side(join.join_type(), true);
            for on in on_predicates {
                if lhs_on_preserved && Self::can_pushdown_predicate(&on, &lhs_schema) {
                    lhs_to_push.push(on);
                } else if rhs_on_preserved && Self::can_pushdown_predicate(&on, &rhs_schema) {
                    rhs_to_push.push(on);
                } else {
                    join_conditions.push(on);
                }
            }
        }

        if lhs_preserved {
            lhs_to_push.extend(Self::extract_join_or_clauses(&keep_predicates, &lhs_schema));
            lhs_to_push.extend(Self::extract_join_or_clauses(&join_conditions, &lhs_schema));
        }
        if rhs_preserved {
            rhs_to_push.extend(Self::extract_join_or_clauses(&keep_predicates, &rhs_schema));
            rhs_to_push.extend(Self::extract_join_or_clauses(&join_conditions, &rhs_schema));
        }

        let new_lhs_plan = match Self::conjunction(lhs_to_push) {
            Some(predicate) => {
                LogicalPlan::Filter(Filter::try_new(Arc::new(join.lhs().clone()), predicate)?)
            }
            None => join.lhs().clone(),
        };
        let new_rhs_plan = match Self::conjunction(rhs_to_push) {
            Some(predicate) => {
                LogicalPlan::Filter(Filter::try_new(Arc::new(join.rhs().clone()), predicate)?)
            }
            None => join.rhs().clone(),
        };

        let new_join_plan = LogicalPlan::Join(Join::new(
            Arc::new(new_lhs_plan),
            Arc::new(new_rhs_plan),
            join.on().to_vec(),
            join.join_type(),
            Self::conjunction(join_conditions),
        ));

        match Self::conjunction(keep_predicates) {
            Some(predicate) => {
                let new_filter =
                    LogicalPlan::Filter(Filter::try_new(Arc::new(new_join_plan), predicate)?);
                Ok(RecursionState::Stop(Some(new_filter)))
            }
            None => Ok(RecursionState::Continue(new_join_plan)),
        }
    }

    fn infer_join_predicates(
        join: &Join,
        predicates: &[Expression],
        on_predicates: &[Expression],
    ) -> Vec<Expression> {
        if join.join_type() != JoinType::Inner {
            return vec![];
        }

        let join_on_columns = join
            .on()
            .iter()
            .flat_map(|(lhs, rhs)| {
                let mut left = HashSet::new();
                let mut right = HashSet::new();
                Self::collect_columns(lhs, &mut left);
                Self::collect_columns(rhs, &mut right);
                Some(left.into_iter().zip(right).collect::<Vec<_>>())
            })
            .flatten()
            .collect::<Vec<_>>();

        let predicate_columns = predicates
            .iter()
            .chain(on_predicates)
            .flat_map(|expr| {
                let mut columns = HashSet::new();
                Self::collect_columns(expr, &mut columns);
                columns.into_iter().collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let mut result: Vec<Expression> = Vec::new();
        for pred_col in &predicate_columns {
            for (left, right) in &join_on_columns {
                if pred_col == left {
                    result.push(Expression::Column(right.clone()));
                    break;
                } else if pred_col == right {
                    result.push(Expression::Column(left.clone()));
                    break;
                }
            }
        }
        result
    }

    fn preserved_join_side(join_type: JoinType, is_on: bool) -> (bool, bool) {
        match join_type {
            JoinType::Inner => (true, true),
            JoinType::Left => (!is_on, is_on),
        }
    }

    fn can_pushdown_predicate(predicate: &Expression, schema: &Schema) -> bool {
        let schema_columns = schema
            .fields()
            .iter()
            .map(|f| Column::new(f.name()))
            .collect::<HashSet<_>>();
        let mut columns = HashSet::new();
        Self::collect_columns(predicate, &mut columns);

        schema_columns
            .intersection(&columns)
            .collect::<HashSet<_>>()
            .len()
            == columns.len()
    }

    fn extract_join_or_clauses<'a>(
        predicates: &'a [Expression],
        schema: &'a Schema,
    ) -> impl Iterator<Item = Expression> + 'a {
        let schema_columns = schema
            .fields()
            .iter()
            .map(|f| Column::new(f.name()))
            .collect::<HashSet<_>>();
        predicates.iter().filter_map(move |expr| {
            if let Expression::Binary(e) = expr {
                if e.op() != &Operator::Or {
                    return None;
                }
                let lhs = Self::extract_or_clause(e.lhs(), &schema_columns);
                let rhs = Self::extract_or_clause(e.rhs(), &schema_columns);
                if let (Some(left), Some(right)) = (lhs, rhs) {
                    return Some(left.or(right));
                }
            }
            None
        })
    }

    fn extract_or_clause(
        expression: &Expression,
        schema_columns: &HashSet<Column>,
    ) -> Option<Expression> {
        let mut predicate = None;

        match expression {
            Expression::Binary(e) if e.op() == &Operator::Or => {
                let lhs = Self::extract_or_clause(e.lhs(), schema_columns);
                let rhs = Self::extract_or_clause(e.rhs(), schema_columns);
                if let (Some(left), Some(right)) = (lhs, rhs) {
                    predicate = Some(left.or(right))
                }
            }
            Expression::Binary(e) if e.op() == &Operator::And => {
                let lhs = Self::extract_or_clause(e.lhs(), schema_columns);
                let rhs = Self::extract_or_clause(e.rhs(), schema_columns);

                match (lhs, rhs) {
                    (Some(left), Some(right)) => predicate = Some(left.and(right)),
                    (Some(left), None) => predicate = Some(left),
                    (None, Some(right)) => predicate = Some(right),
                    (None, None) => predicate = None,
                }
            }
            _ => {
                let mut columns = HashSet::new();
                Self::collect_columns(expression, &mut columns);
                if schema_columns
                    .intersection(&columns)
                    .collect::<HashSet<_>>()
                    .len()
                    == columns.len()
                {
                    predicate = Some(expression.clone())
                }
            }
        }

        predicate
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
        expression::logical::{
            expr::Expression,
            expr_fn::{col, lit, sort, sum},
        },
        io::reader::csv::{options::CsvReadOptions, source::CsvDataSource},
        optimize::rules::{predicate_pushdown::PredicatePushDownRule, OptimizerRule},
        plan::logical::{
            aggregate::Aggregate,
            filter::Filter,
            join::{Join, JoinType},
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

    fn create_filter(input: Arc<LogicalPlan>, predicate: Expression) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Filter(
            Filter::try_new(input, predicate).unwrap(),
        ))
    }

    #[test]
    fn test_predicate_push_down_join() {
        let lhs = create_scan();
        let predicate = col("c2").eq(lit(4i64));
        let lhs = create_filter(lhs, predicate);
        let rhs = create_scan();

        let input = Arc::new(LogicalPlan::Join(Join::new(
            lhs,
            rhs,
            vec![(col("c1"), col("c1"))],
            JoinType::Left,
            None,
        )));
        let predicate = col("c2").eq(lit(5i64));
        let input = create_filter(input, predicate);

        println!("{input}");

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        println!("{result}");
    }

    #[test]
    fn test_predicate_pushdown_multiple_nested_filters_with_aggregate_keep() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Projection(Projection::new(
            input,
            vec![col("c2")],
        )));
        let predicate = col("c2").eq(lit(5i64));
        let input = create_filter(input, predicate);
        let input = Arc::new(LogicalPlan::Aggregate(
            Aggregate::try_new(input, vec![col("c2")], vec![sum(col("c3"))]).unwrap(),
        ));
        let input = Arc::new(LogicalPlan::Sort(Sort::new(
            input,
            vec![sort(col("c2"), true)],
        )));
        let predicate = col("SUM(c3)").gt(lit(3i64));
        let input = create_filter(input, predicate);

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Sort: [Sort[c2]]\n\tFilter: [SUM(c3) > 3]\n\t\tAggregate: groupBy:[c2]; aggrExprs:[SUM(c3)]\n\t\t\tFilter: [c2 = 5]\n\t\t\t\tProjection: [c2]\n\t\t\t\t\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_pushdown_multiple_nested_filters_with_aggregate() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Projection(Projection::new(
            input,
            vec![col("c2")],
        )));
        let predicate = col("c2").eq(lit(5i64));
        let input = create_filter(input, predicate);
        let input = Arc::new(LogicalPlan::Aggregate(
            Aggregate::try_new(input, vec![col("c2")], vec![sum(col("c3"))]).unwrap(),
        ));
        let input = Arc::new(LogicalPlan::Sort(Sort::new(
            input,
            vec![sort(col("c2"), true)],
        )));
        let predicate = col("c2").gt(lit(3i64));
        let input = create_filter(input, predicate);

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Sort: [Sort[c2]]\n\tAggregate: groupBy:[c2]; aggrExprs:[SUM(c3)]\n\t\tProjection: [c2]\n\t\t\tFilter: [c2 > 3 AND c2 = 5]\n\t\t\t\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }

    #[test]
    fn test_predicate_push_down_aggregate() {
        let input = create_scan();
        let input = Arc::new(LogicalPlan::Aggregate(
            Aggregate::try_new(input, vec![col("c2")], vec![sum(col("c3"))]).unwrap(),
        ));
        let predicate = col("c2").gt(lit(5i64));
        let input = create_filter(input, predicate);

        let rule = PredicatePushDownRule::new();
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

        let rule = PredicatePushDownRule::new();
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

        let rule = PredicatePushDownRule::new();
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

        let rule = PredicatePushDownRule::new();
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

        let rule = PredicatePushDownRule::new();
        let result = rule.try_optimize(&input).unwrap().unwrap();
        assert_eq!(
            format!("{}", result),
            "Filter: [c2 = 5]\n\tScan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        );
    }
}

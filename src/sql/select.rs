use std::{collections::HashMap, sync::Arc};

use arrow_array::{Datum, Int64Array};
use snafu::location;
use sqlparser::ast::{
    Distinct as SQLDistinct, Expr, GroupByExpr, ObjectName, OrderBy, Query, SelectItem, SetExpr,
    TableFactor, TableWithJoins,
};

use crate::{
    error::{Error, Result},
    expression::logical::{
        expr::Expression,
        expr_fn::{col, sort},
    },
    io::reader::listing::table::ListingTable,
    plan::logical::{
        aggregate::Aggregate, distinct::Distinct, filter::Filter, limit::Limit, plan::LogicalPlan,
        projection::Projection, scan::Scan, sort::Sort,
    },
    sql::expr::sql_expr_to_logical_expr,
};

use super::join::parse_join_relation;

/// Converts a SQL `Query` to a `LogicalPlan`.
pub fn query_to_plan(query: Query, tables: &HashMap<String, ListingTable>) -> Result<LogicalPlan> {
    let body = *query.body;
    let plan = match body {
        SetExpr::Select(select) => {
            let plan = plan_from_tables(select.from, tables)?;
            let plan = plan_from_selection(plan, select.selection)?;
            let plan = plan_from_distinct(plan, select.distinct.as_ref())?;
            let plan = plan_from_aggregation(plan, &select.projection, select.group_by)?;
            plan_from_projection(plan, &select.projection)?
        }
        _ => {
            return Err(Error::InvalidOperation {
                message: "Only select statements are supported".to_string(),
                location: location!(),
            })
        }
    };
    let plan = plan_from_order_by(plan, query.order_by)?;
    let plan = plan_from_limit(plan, query.limit)?;

    Ok(plan)
}

/// Processes the `LIMIT` clause of a SQL query to generate a `LogicalPlan` with the `Limit` node.
fn plan_from_limit(plan: LogicalPlan, limit: Option<Expr>) -> Result<LogicalPlan> {
    match limit {
        None => Ok(plan),
        Some(e) => {
            let limit_expr = sql_expr_to_logical_expr(&e)?;
            let fetch = if let Expression::Literal(lit) = limit_expr {
                let scalar = lit.to_scalar()?;
                let (arr, _) = scalar.get();
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("failed to downcast array");
                let n = arr.value(0);
                Some(n as usize)
            } else {
                None
            };

            Ok(LogicalPlan::Limit(Limit::new(Arc::new(plan), 0, fetch)))
        }
    }
}

/// Processes the `ORDER BY` clause of a SQL query to generate a `LogicalPlan` with the `Sort` node.
fn plan_from_order_by(plan: LogicalPlan, order_by: Option<OrderBy>) -> Result<LogicalPlan> {
    match order_by {
        None => Ok(plan),
        Some(OrderBy { exprs, .. }) => {
            let mut expression = Vec::with_capacity(exprs.len());
            for expr in &exprs {
                let sort_col = sql_expr_to_logical_expr(&expr.expr)?;
                if let Expression::Column(_) = sort_col {
                    let ascending = expr.asc.unwrap_or(true);
                    expression.push(sort(sort_col, ascending));
                }
            }

            Ok(LogicalPlan::Sort(Sort::new(Arc::new(plan), expression)))
        }
    }
}

/// Processes the `DISTINCT` clause of a SQL query to generate a `LogicalPlan` with the `Distinct` node.
fn plan_from_distinct(plan: LogicalPlan, distinct: Option<&SQLDistinct>) -> Result<LogicalPlan> {
    match distinct {
        None => Ok(plan),
        Some(dist) => match dist {
            SQLDistinct::Distinct => Ok(LogicalPlan::Distinct(Distinct::new(Arc::new(plan)))),
            SQLDistinct::On(_) => Err(Error::InvalidOperation {
                message: "SQL distinct on is not supported yet".to_string(),
                location: location!(),
            }),
        },
    }
}

/// Processes the `GROUP BY` and aggregation functions in a SQL query
/// to generate a `LogicalPlan` with the `Aggregate` node.
fn plan_from_aggregation(
    plan: LogicalPlan,
    projection: &[SelectItem],
    sql_group_by: GroupByExpr,
) -> Result<LogicalPlan> {
    let mut group_by = vec![];
    if let GroupByExpr::Expressions(exprs, _) = &sql_group_by {
        for expr in exprs {
            group_by.push(sql_expr_to_logical_expr(expr)?);
        }
    }

    let mut aggregate_expressions = vec![];
    for proj in projection {
        if let SelectItem::UnnamedExpr(e) = proj {
            let expr = sql_expr_to_logical_expr(e)?;
            if let Expression::Aggregate(_) = &expr {
                aggregate_expressions.push(expr);
            }
        }
    }

    if group_by.is_empty() && aggregate_expressions.is_empty() {
        return Ok(plan);
    }

    let plan = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(plan),
        group_by,
        aggregate_expressions,
    )?);

    Ok(plan)
}

/// Processes the projection list in a SQL query to generate a `LogicalPlan` with the `Projection` node.
fn plan_from_projection(plan: LogicalPlan, projection: &[SelectItem]) -> Result<LogicalPlan> {
    let schema = plan.schema();

    let mut projected_cols = Vec::with_capacity(projection.len());
    for proj in projection {
        match proj {
            SelectItem::Wildcard(_) => {
                for field in schema.fields() {
                    projected_cols.push(col(field.name()));
                }
            }
            SelectItem::UnnamedExpr(e) => {
                let expr = sql_expr_to_logical_expr(e)?;
                if let Expression::Column(_) = expr {
                    projected_cols.push(expr);
                }
            }
            _ => {
                return Err(Error::InvalidOperation {
                    message: format!("SQL projection expr {} is not supported yet", proj)
                        .to_string(),
                    location: location!(),
                })
            }
        }
    }

    let plan = LogicalPlan::Projection(Projection::new(Arc::new(plan), projected_cols));

    Ok(plan)
}

/// Processes the `WHERE` clause of a SQL query to generate a `LogicalPlan` with the `Filter` node.
fn plan_from_selection(plan: LogicalPlan, selection: Option<Expr>) -> Result<LogicalPlan> {
    match selection {
        Some(expr) => {
            let predicate = sql_expr_to_logical_expr(&expr)?;
            let plan = LogicalPlan::Filter(Filter::try_new(Arc::new(plan), predicate)?);

            Ok(plan)
        }
        None => Ok(plan),
    }
}

/// Processes the `FROM` clause of a SQL query to generate a `LogicalPlan` from the listed tables and joins.
fn plan_from_tables(
    mut from: Vec<TableWithJoins>,
    tables: &HashMap<String, ListingTable>,
) -> Result<LogicalPlan> {
    match from.len() {
        1 => {
            let table = from.remove(0);

            let mut lhs = create_relation(table.relation, tables)?;

            for join in table.joins.into_iter() {
                lhs = parse_join_relation(lhs, join, tables)?;
            }

            Ok(lhs)
        }
        _ => Err(Error::InvalidOperation {
            message: "SQL FROM clause only supports a single relation".to_string(),
            location: location!(),
        }),
    }
}

/// Creates a `LogicalPlan` from a table relation in the SQL `FROM` clause.
pub fn create_relation(
    relation: TableFactor,
    tables: &HashMap<String, ListingTable>,
) -> Result<LogicalPlan> {
    match relation {
        TableFactor::Table { name, .. } => {
            let ObjectName(idents) = name;
            let table_name = match idents.len() {
                1 => &idents[0].value,
                _ => {
                    return Err(Error::InvalidOperation {
                        message: "Only single, bare table references are allowed.".to_string(),
                        location: location!(),
                    })
                }
            };
            let listing_table = tables
                .get(table_name)
                .expect("listing_table does not exist");
            let path = listing_table.path();
            let listing_table = Arc::new(listing_table.clone());

            let plan = LogicalPlan::Scan(Scan::new(path, listing_table, None, vec![]));

            Ok(plan)
        }
        _ => Err(Error::InvalidOperation {
            message: format!("Operation {} is not supported yet", relation),
            location: location!(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use sqlparser::ast::Statement;

    use crate::{
        io::{reader::listing::table::ListingTable, FileFormat},
        sql::parser::WrappedParser,
        tests::create_schema,
    };

    use super::query_to_plan;

    fn create_single_table() -> HashMap<String, ListingTable> {
        let schema = create_schema();
        let listing_table = ListingTable::new(
            "simple",
            "testdata/csv/simple.csv",
            Arc::new(schema),
            FileFormat::Csv,
        );
        HashMap::from([("simple".to_string(), listing_table)])
    }

    #[test]
    fn test_query_to_plan_select_with_limit() {
        let tables = create_single_table();
        let sql = "SELECT c1, c2 FROM simple LIMIT 5";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = match statement {
            Statement::Query(query) => query_to_plan(*query, &tables).unwrap(),
            _ => panic!(),
        };

        assert_eq!(
            format!("{}", result),
            "Limit: [skip: 0, fetch:Some(5)]\n\t\
                Projection: [c1, c2]\n\t\t\
                    Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }

    #[test]
    fn test_query_to_plan_select_with_order_by() {
        let tables = create_single_table();
        let sql = "SELECT c1, c2 FROM simple ORDER BY c2, c1";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = match statement {
            Statement::Query(query) => query_to_plan(*query, &tables).unwrap(),
            _ => panic!(),
        };

        assert_eq!(
            format!("{}", result),
            "Sort: [Sort[c2], Sort[c1]]\n\t\
                Projection: [c1, c2]\n\t\t\
                    Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }

    #[test]
    fn test_query_to_plan_select_with_distinct() {
        let tables = create_single_table();
        let sql = "SELECT DISTINCT c1 FROM simple";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = match statement {
            Statement::Query(query) => query_to_plan(*query, &tables).unwrap(),
            _ => panic!(),
        };

        assert_eq!(
            format!("{}", result),
            "Distinct\n\t\
                Projection: [c1]\n\t\t\
                    Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }

    #[test]
    fn test_query_to_plan_select_with_aggregation() {
        let tables = create_single_table();
        let sql = "SELECT c1, SUM(c2), MIN(c3) FROM simple GROUP BY c1";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = match statement {
            Statement::Query(query) => query_to_plan(*query, &tables).unwrap(),
            _ => panic!(),
        };

        assert_eq!(
            format!("{}", result),
            "Projection: [c1]\n\t\
                Aggregate: groupBy:[c1]; aggrExprs:[SUM(c2), MIN(c3)]\n\t\t\
                    Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }

    #[test]
    fn test_query_to_plan_select_with_projection() {
        let tables = create_single_table();
        let sql = "SELECT c1, c3 FROM simple";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = match statement {
            Statement::Query(query) => query_to_plan(*query, &tables).unwrap(),
            _ => panic!(),
        };

        assert_eq!(
            format!("{}", result),
            "Projection: [c1, c3]\n\t\
                Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }

    #[test]
    fn test_query_to_plan_select_with_filter() {
        let tables = create_single_table();
        let sql = "SELECT * FROM simple WHERE c1 = 'a'";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = match statement {
            Statement::Query(query) => query_to_plan(*query, &tables).unwrap(),
            _ => panic!(),
        };

        assert_eq!(
            format!("{}", result),
            "Projection: [c1, c2, c3]\n\t\
                Filter: [c1 = a]\n\t\t\
                    Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }

    #[test]
    fn test_query_to_plan_select_with_join() {
        let schema = Arc::new(create_schema());
        let lhs = ListingTable::new(
            "lhs",
            "testdata/csv/simple.csv",
            schema.clone(),
            FileFormat::Csv,
        );
        let rhs = ListingTable::new("rhs", "testdata/csv/simple.csv", schema, FileFormat::Csv);
        let tables = HashMap::from([("lhs".to_string(), lhs), ("rhs".to_string(), rhs)]);

        let sql = "SELECT * FROM lhs LEFT JOIN rhs ON c1 = c1";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = match statement {
            Statement::Query(query) => query_to_plan(*query, &tables).unwrap(),
            _ => panic!(),
        };

        assert_eq!(
            format!("{}", result),
            "Projection: [c1, c2, c3, c1, c2, c3]\n\t\
                Join: [type: LEFT, on: [(Column(Column { name: \"c1\" }), Column(Column { name: \"c1\" }))]]\n\t\t\
                    Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n\t\t\
                    Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }

    #[test]
    fn test_query_to_plan_select_all() {
        let tables = create_single_table();
        let sql = "SELECT * FROM simple";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = match statement {
            Statement::Query(query) => query_to_plan(*query, &tables).unwrap(),
            _ => panic!(),
        };

        assert_eq!(
            format!("{}", result),
            "Projection: [c1, c2, c3]\n\t\
                Scan: testdata/csv/simple.csv; projection=None; filter=[[]]\n"
        )
    }
}

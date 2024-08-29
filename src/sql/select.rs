use std::{collections::HashMap, sync::Arc};

use snafu::location;
use sqlparser::ast::{Expr, ObjectName, Query, SelectItem, SetExpr, TableFactor, TableWithJoins};

use crate::{
    error::{Error, Result},
    expression::logical::expr_fn::col,
    io::reader::listing::table::ListingTable,
    plan::logical::{filter::Filter, plan::LogicalPlan, projection::Projection, scan::Scan},
    sql::expr::sql_expr_to_logical_expr,
};

use super::join::parse_join_relation;

pub fn query_to_plan(query: Query, tables: &HashMap<String, ListingTable>) -> Result<LogicalPlan> {
    let query = *query.body;
    match query {
        SetExpr::Select(select) => {
            // process select exprs
            // process projection
            // process order-by
            // process disctinct
            // apply limit

            let plan = plan_from_tables(select.from, tables)?;
            let plan = plan_from_selection(plan, select.selection)?;
            let plan = plan_from_projection(plan, select.projection)?;

            Ok(plan)
        }
        _ => Err(Error::InvalidOperation {
            message: "Only select statements are supported".to_string(),
            location: location!(),
        }),
    }
}

fn plan_from_projection(plan: LogicalPlan, projection: Vec<SelectItem>) -> Result<LogicalPlan> {
    let schema = plan.schema();

    let mut projected_cols = Vec::with_capacity(projection.len());
    for proj in &projection {
        match proj {
            SelectItem::Wildcard(_) => {
                for field in schema.fields() {
                    projected_cols.push(col(field.name()));
                }
            }
            SelectItem::UnnamedExpr(e) => {
                let expr = sql_expr_to_logical_expr(e)?;
                projected_cols.push(expr);
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

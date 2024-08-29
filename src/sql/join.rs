use std::{collections::HashMap, sync::Arc};

use snafu::location;
use sqlparser::ast::{Join as SQLJoin, JoinConstraint, JoinOperator};

use crate::{
    error::{Error, Result},
    expression::logical::expr::Expression,
    io::reader::listing::table::ListingTable,
    plan::logical::{
        join::{Join, JoinType},
        plan::LogicalPlan,
    },
};

use super::{expr::sql_expr_to_logical_expr, select::create_relation};

pub fn parse_join_relation(
    lhs: LogicalPlan,
    join: SQLJoin,
    tables: &HashMap<String, ListingTable>,
) -> Result<LogicalPlan> {
    let rhs = create_relation(join.relation, tables)?;
    match join.join_operator {
        JoinOperator::Inner(constraint) => parse_join(lhs, rhs, constraint, JoinType::Inner),
        JoinOperator::LeftOuter(constraint) => parse_join(lhs, rhs, constraint, JoinType::Left),
        _ => Err(Error::InvalidOperation {
            message: format!("JoinOperator {:?} is not supported yet", join.join_operator),
            location: location!(),
        }),
    }
}

pub fn parse_join(
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    constraint: JoinConstraint,
    join_type: JoinType,
) -> Result<LogicalPlan> {
    match constraint {
        JoinConstraint::On(sql_expr) => {
            let on = if let Expression::Binary(expr) = sql_expr_to_logical_expr(&sql_expr)? {
                vec![(expr.lhs().clone(), expr.rhs().clone())]
            } else {
                vec![]
            };

            let plan = LogicalPlan::Join(Join::try_new(
                Arc::new(lhs),
                Arc::new(rhs),
                on,
                join_type,
                None,
            )?);

            Ok(plan)
        }
        _ => Err(Error::InvalidOperation {
            message: format!("JoinConstraint {:?} is not supported yet", constraint),
            location: location!(),
        }),
    }
}

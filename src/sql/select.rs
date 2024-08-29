use sqlparser::ast::{Query, SetExpr};

use crate::{error::Result, plan::logical::plan::LogicalPlan};

pub fn query_to_plan(query: Query) -> Result<LogicalPlan> {
    let query = *query.body;
    match query {
        SetExpr::Select(mut _select) => {
            // process from clause
            // process where clause
            // process select exprs
            // process projection
            // process order-by
            // process disctinct
            todo!()
        }
        _ => todo!(),
    }
}

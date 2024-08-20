use crate::{error::Result, plan::logical::plan::LogicalPlan};

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
            // TODO:
            LogicalPlan::Scan(_plan) => None,
            _ => None,
        };

        Ok(new_plan)
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
mod tests {}

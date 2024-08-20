use std::sync::Arc;

use crate::{error::Result, plan::logical::plan::LogicalPlan};

use super::rules::OptimizerRule;

#[derive(Debug)]
pub struct Optimizer {
    rules: Vec<Arc<dyn OptimizerRule>>,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    pub fn new() -> Self {
        let rules = vec![];

        Self::with_rules(rules)
    }

    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule>>) -> Self {
        Self { rules }
    }

    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut new_plan = plan.clone();
        for rule in &self.rules {
            if let Some(optimized) = rule.try_optimize(&new_plan)? {
                new_plan = optimized;
            };
        }

        Ok(new_plan)
    }
}

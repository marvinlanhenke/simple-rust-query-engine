use std::sync::Arc;

use crate::{error::Result, plan::logical::plan::LogicalPlan};

use super::rules::{
    predicate_pushdown::PredicatePushDownRule, projection_pushdown::ProjectionPushDownRule,
    OptimizerRule,
};

/// Represents the query optimizer, which applies a series of optimization rules
/// to a logical plan in order to improve its performance or execution efficiency.
#[derive(Debug)]
pub struct Optimizer {
    /// A list of optimization rules to be applied.
    rules: Vec<Arc<dyn OptimizerRule>>,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Creates a new `Optimizer` instance with a default set of optimization rules.
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn OptimizerRule>> = vec![
            Arc::new(PredicatePushDownRule::new()),
            Arc::new(ProjectionPushDownRule::new()),
        ];

        Self::with_rules(rules)
    }

    /// Creates a new `Optimizer` instance with a custom set of optimization rules.
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule>>) -> Self {
        Self { rules }
    }

    /// Optimizes the provided logical plan by applying each of the optimization rules sequentially.
    ///
    /// The optimizer applies the rules in the order they are listed. If a rule produces
    /// an optimized plan, that plan is used as the input for the next rule in the sequence.
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

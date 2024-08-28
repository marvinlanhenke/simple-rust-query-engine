use std::fmt::Debug;

use crate::{error::Result, plan::logical::plan::LogicalPlan};

pub mod predicate_pushdown;
pub mod projection_pushdown;
pub mod rewrite_distinct;
pub mod utils;

/// A trait that defines a rule for optimizing logical plans.
///
/// Implementations of this trait define specific optimization strategies
/// that can be applied to a logical plan to improve its performance or efficiency.
pub trait OptimizerRule: Debug {
    /// Returns the name of the optimization rule.
    fn name(&self) -> &str;

    /// Attempts to optimize the given logical plan.
    ///
    /// The rule may or may not modify the plan. If the rule can optimize the plan,
    /// it returns an `Option` containing the optimized plan. If the plan cannot be optimized
    /// by this rule, it returns `None`.
    fn try_optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>>;
}

/// Represents the state of the recursion during predicate pushdown.
pub enum RecursionState {
    /// Indicates that the recursion should continue with a given `LogicalPlan`.
    Continue(LogicalPlan),
    /// Indicates that the recursion should stop, returning an optional `LogicalPlan`.
    Stop(Option<LogicalPlan>),
}

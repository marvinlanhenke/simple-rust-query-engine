use std::fmt::Debug;

use crate::{error::Result, plan::logical::plan::LogicalPlan};

pub mod predicate_pushdown;
pub mod projection_pushdown;
pub mod rewrite_distinct;
pub mod type_coercion;

pub trait OptimizerRule: Debug {
    fn name(&self) -> &str;

    fn try_optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>>;
}

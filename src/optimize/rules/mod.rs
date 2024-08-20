use std::fmt::Debug;

use crate::{error::Result, plan::logical::plan::LogicalPlan};

pub trait OptimizerRule: Debug {
    fn name(&self) -> &str;

    fn try_optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>>;
}

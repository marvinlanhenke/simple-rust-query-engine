use std::sync::Arc;

use crate::{error::Result, plan::logical::plan::LogicalPlan};

use super::plan::ExecutionPlan;

/// The query [`Planner`].
///
/// Responsible for translating logical to physical plans.
pub struct Planner;

impl Planner {
    /// Attempts to create a [`ExecutionPlan`] from the provided input [`LogicalPlan`].
    pub fn create_physical_plan(input: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        use LogicalPlan::*;

        match input {
            Scan(plan) => {
                let source = plan.source();
                let projection = plan.projection();
                source.scan(projection)
            }
        }
    }
}

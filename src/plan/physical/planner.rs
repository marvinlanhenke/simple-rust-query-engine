use std::sync::Arc;

use crate::{error::Result, plan::logical::plan::LogicalPlan};

use super::plan::ExecutionPlan;

pub struct Planner;

impl Planner {
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

use std::sync::Arc;

use arrow::datatypes::Schema;
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::{
        logical::expr::Expression,
        physical::{column::ColumnExpr, expr::PhysicalExpression},
    },
    plan::{logical::plan::LogicalPlan, physical::projection::ProjectionExec},
};

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
            Projection(plan) => {
                let input = Self::create_physical_plan(plan.input())?;

                let mut fields = Vec::with_capacity(plan.expressions().len());
                let mut expression = Vec::with_capacity(plan.expressions().len());
                for curr_expr in plan.expressions().iter() {
                    let field = curr_expr.to_field(plan.input())?;
                    let expr = Self::create_physical_expression(plan.input(), curr_expr)?;
                    fields.push(field);
                    expression.push(expr);
                }
                let schema = Arc::new(Schema::new(fields));

                Ok(Arc::new(ProjectionExec::new(input, schema, expression)))
            }
        }
    }

    /// Converts a logical to a physical expression.
    fn create_physical_expression(
        input: &LogicalPlan,
        expr: &Expression,
    ) -> Result<Arc<dyn PhysicalExpression>> {
        use Expression::*;

        match expr {
            Column(e) => {
                let (index, _) = input.schema().column_with_name(e.name()).ok_or_else(|| {
                    Error::InvalidData {
                        message: format!(
                            "Column with name '{}' could not be found in schema",
                            e.name()
                        ),
                        location: location!(),
                    }
                })?;
                Ok(Arc::new(ColumnExpr::new(index)))
            }
            Literal(_) => todo!(),
        }
    }
}

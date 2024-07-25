use std::{
    any::Any,
    fmt::{write, Display},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;

use crate::{error::Result, expression::physical::expr::PhysicalExpression, io::RecordBatchStream};

use super::plan::{format_exec, ExecutionPlan};

#[derive(Debug)]
pub struct ProjectionExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    expression: Vec<Arc<dyn PhysicalExpression>>,
}

impl ProjectionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        expression: Vec<Arc<dyn PhysicalExpression>>,
    ) -> Self {
        Self {
            input,
            schema,
            expression,
        }
    }
}

impl ExecutionPlan for ProjectionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&dyn ExecutionPlan> {
        vec![self.input.as_ref()]
    }

    fn execute(&self) -> Result<RecordBatchStream> {
        let _input = self.input.execute()?;
        // create a new stream with projected cols here
        todo!()
    }

    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProjectionExec: {:?}", self.expression)
    }
}

impl Display for ProjectionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_exec(self, f, 0)
    }
}

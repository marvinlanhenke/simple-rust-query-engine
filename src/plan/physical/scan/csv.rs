use std::{any::Any, fmt::Display};

use arrow::datatypes::SchemaRef;

use crate::{
    error::Result,
    io::{
        reader::csv::{opener::CsvFileOpener, options::CsvFileOpenerConfig},
        FileOpener, RecordBatchStream,
    },
    plan::physical::plan::ExecutionPlan,
};

#[derive(Debug)]
pub struct CsvExec {
    path: String,
    config: CsvFileOpenerConfig,
}

impl CsvExec {
    pub fn new(path: impl Into<String>, config: CsvFileOpenerConfig) -> Self {
        Self {
            path: path.into(),
            config,
        }
    }
}

impl ExecutionPlan for CsvExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.config.schema()
    }

    fn children(&self) -> Vec<&dyn ExecutionPlan> {
        vec![]
    }

    fn execute(&self) -> Result<RecordBatchStream> {
        let opener = CsvFileOpener::new(self.config.clone());
        opener.open(&self.path)
    }
}

impl Display for CsvExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.config.projection() {
            None => write!(
                f,
                "CsvExec: schema={}, projection=None",
                self.config.schema()
            ),
            Some(projection) => write!(
                f,
                "CsvExec: schema={}, projection=[{:?}]",
                self.config.schema(),
                projection
            ),
        }
    }
}

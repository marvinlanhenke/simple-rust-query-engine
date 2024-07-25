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

    pub fn path(&self) -> &str {
        self.path.as_ref()
    }

    pub fn config(&self) -> &CsvFileOpenerConfig {
        &self.config
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
        let opener = CsvFileOpener::new(&self.config);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::{
        io::reader::csv::options::CsvFileOpenerConfig,
        plan::physical::{plan::ExecutionPlan, scan::csv::CsvExec},
        tests::create_schema,
    };

    #[tokio::test]
    async fn test_csv_exec_with_projection() {
        let schema = Arc::new(create_schema());
        let projection = Some(vec![0]);
        let config = CsvFileOpenerConfig::builder(schema)
            .with_batch_size(1)
            .with_projection(projection)
            .build();
        let exec = CsvExec::new("testdata/csv/simple.csv", config);
        let mut stream = exec.execute().unwrap();

        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 1);
        }
    }

    #[tokio::test]
    async fn test_csv_exec_no_projection() {
        let schema = Arc::new(create_schema());
        let config = CsvFileOpenerConfig::builder(schema)
            .with_batch_size(1)
            .build();
        let exec = CsvExec::new("testdata/csv/simple.csv", config);
        let mut stream = exec.execute().unwrap();

        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 3);
        }
    }
}

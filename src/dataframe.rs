use arrow::{array::RecordBatch, util::pretty};
use futures::TryStreamExt;

use crate::{
    error::Result,
    plan::{logical::plan::LogicalPlan, physical::planner::Planner},
};

/// Represents a [`DataFrame`] for query execution and data manipulation.
#[derive(Debug)]
pub struct DataFrame {
    /// The [`LogicalPlan`] for the [`DataFrame`].
    plan: LogicalPlan,
}

impl DataFrame {
    /// Creates a new [`DataFrame`] instance.
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    /// Displays the dataframe's content in a tabular format.
    pub async fn show(&self) -> Result<()> {
        let results = self.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Collects the results of the query execution as `RecordBatch`'es.
    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let physical_plan = Planner::create_physical_plan(&self.plan)?;
        let stream = physical_plan.execute()?;

        stream.try_collect().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{context::SessionContext, io::reader::csv::options::CsvReadOptions};

    #[tokio::test]
    async fn test_dataframe_collect_csv() {
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv("testdata/csv/simple.csv", CsvReadOptions::new())
            .unwrap();

        let result = df.collect().await.unwrap();

        assert_eq!(result[0].num_rows(), 6);
        assert_eq!(result[0].num_columns(), 3);
    }
}

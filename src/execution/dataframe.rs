use std::sync::Arc;

use arrow::{array::RecordBatch, util::pretty};
use futures::TryStreamExt;

use crate::{
    error::Result,
    expression::logical::expr::Expression,
    plan::{
        logical::{filter::Filter, plan::LogicalPlan, projection::Projection},
        planner::Planner,
    },
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

    /// Projects the selected columns on the [`DataFrame`].
    pub fn select(self, columns: Vec<Expression>) -> Self {
        let input = self.plan;
        let plan = LogicalPlan::Projection(Projection::new(Arc::new(input), columns));

        Self { plan }
    }

    /// Applies a filter predicate on the [`DataFrame`].
    pub fn filter(self, predicate: Expression) -> Result<Self> {
        let input = self.plan;
        let plan = LogicalPlan::Filter(Filter::try_new(Arc::new(input), predicate)?);

        Ok(Self { plan })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        execution::context::SessionContext,
        expression::logical::expr_fn::{col, lit},
        io::reader::csv::options::CsvReadOptions,
    };

    #[tokio::test]
    async fn test_dataframe_filter_csv() {
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv("testdata/csv/simple.csv", CsvReadOptions::new())
            .unwrap()
            .select(vec![col("c1"), col("c3")])
            .filter(col("c1").neq(lit("a")))
            .unwrap();

        let result = df.collect().await.unwrap();

        assert_eq!(result[0].num_rows(), 5);
        assert_eq!(result[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_dataframe_select_csv() {
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv("testdata/csv/simple.csv", CsvReadOptions::new())
            .unwrap()
            .select(vec![col("c1"), col("c3")]);

        let result = df.collect().await.unwrap();

        assert_eq!(result[0].num_rows(), 6);
        assert_eq!(result[0].num_columns(), 2);
    }

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

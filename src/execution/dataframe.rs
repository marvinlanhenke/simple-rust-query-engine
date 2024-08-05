use std::sync::Arc;

use arrow::{array::RecordBatch, util::pretty};
use futures::TryStreamExt;

use crate::{
    error::Result,
    expression::logical::expr::Expression,
    plan::{
        logical::{
            aggregate::Aggregate, filter::Filter, plan::LogicalPlan, projection::Projection,
        },
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

    /// Performs an aggregation operation based on the
    /// provided grouping and aggregation expressions.
    pub fn aggregate(
        self,
        group_by: Vec<Expression>,
        aggregate_expressions: Vec<Expression>,
    ) -> Result<Self> {
        let input = self.plan;
        let plan = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(input),
            group_by,
            aggregate_expressions,
        )?);

        Ok(Self { plan })
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty;

    use crate::{
        execution::context::SessionContext,
        expression::logical::expr_fn::{avg, col, count, lit, max, min, sum},
        io::reader::csv::options::CsvReadOptions,
    };

    use super::DataFrame;

    async fn assert_df_results(df: &DataFrame, expected: Vec<&str>) {
        let results = df.collect().await.unwrap();
        let results = pretty::pretty_format_batches(&results).unwrap().to_string();
        let results = results.trim().lines().collect::<Vec<_>>();
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_dataframe_aggregate_with_groupings() {
        let ctx = SessionContext::new();

        let group_by = vec![col("c1")];
        let aggregate_expressions = vec![
            count(col("c2")),
            sum(col("c2")),
            avg(col("c2")),
            max(col("c2")),
            min(col("c2")),
        ];

        let df = ctx
            .read_csv("testdata/csv/simple_aggregate.csv", CsvReadOptions::new())
            .unwrap()
            .aggregate(group_by, aggregate_expressions)
            .unwrap();

        df.show().await.unwrap();

        let expected = vec![
            "+----+-----------+---------+---------+---------+---------+",
            "| c1 | COUNT(c2) | SUM(c2) | AVG(c2) | MAX(c2) | MIN(c2) |",
            "+----+-----------+---------+---------+---------+---------+",
            "| a  | 2         | 3       | 1.5     | 2       | 1       |",
            "| c  | 2         | 8       | 4.0     | 5       | 3       |",
            "| d  | 1         | 4       | 4.0     | 4       | 4       |",
            "| f  | 1         | 6       | 6.0     | 6       | 6       |",
            "| b  | 1         | 7       | 7.0     | 7       | 7       |",
            "+----+-----------+---------+---------+---------+---------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_aggregate_no_groupings() {
        let ctx = SessionContext::new();

        let group_by = vec![];
        let aggregate_expressions = vec![
            count(col("c1")),
            sum(col("c2")),
            avg(col("c2")),
            max(col("c2")),
            min(col("c2")),
        ];

        let df = ctx
            .read_csv("testdata/csv/simple.csv", CsvReadOptions::new())
            .unwrap()
            .filter(col("c1").neq(lit("a")))
            .unwrap()
            .aggregate(group_by, aggregate_expressions)
            .unwrap();

        let expected = vec![
            "+-----------+---------+---------+---------+---------+",
            "| COUNT(c1) | SUM(c2) | AVG(c2) | MAX(c2) | MIN(c2) |",
            "+-----------+---------+---------+---------+---------+",
            "| 5         | 20      | 4.0     | 6       | 2       |",
            "+-----------+---------+---------+---------+---------+",
        ];
        assert_df_results(&df, expected).await;
    }

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

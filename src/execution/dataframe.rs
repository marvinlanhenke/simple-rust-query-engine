use std::sync::Arc;

use arrow::{array::RecordBatch, util::pretty};
use futures::TryStreamExt;

use crate::{
    error::Result,
    expression::logical::{expr::Expression, expr_fn::col},
    optimize::optimizer::Optimizer,
    plan::{
        logical::{
            aggregate::Aggregate,
            filter::Filter,
            join::{Join, JoinType},
            limit::Limit,
            plan::LogicalPlan,
            projection::Projection,
            sort::Sort,
        },
        planner::Planner,
    },
};

/// Represents a [`DataFrame`] for query execution and data manipulation.
#[derive(Debug)]
pub struct DataFrame {
    /// The [`LogicalPlan`] for the [`DataFrame`].
    plan: LogicalPlan,
    /// The [`Optimizer`] responsible
    /// for rewriting a logical plan.
    optimizer: Optimizer,
}

impl DataFrame {
    /// Creates a new [`DataFrame`] instance.
    pub fn new(plan: LogicalPlan) -> Self {
        Self {
            plan,
            optimizer: Optimizer::new(),
        }
    }

    /// Displays the dataframe's content in a tabular format.
    pub async fn show(&self) -> Result<()> {
        let results = self.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Collects the results of the query execution as `RecordBatch`'es.
    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let optimized = self.optimizer.optimize(self.plan.clone())?;
        let physical_plan = Planner::create_physical_plan(&optimized)?;
        let stream = physical_plan.execute()?;

        stream.try_collect().await
    }

    /// Projects the selected columns on the [`DataFrame`].
    pub fn select(self, columns: Vec<Expression>) -> Self {
        let input = self.plan;
        let plan = LogicalPlan::Projection(Projection::new(Arc::new(input), columns));

        Self {
            plan,
            optimizer: self.optimizer,
        }
    }

    /// Applies a filter predicate on the [`DataFrame`].
    pub fn filter(self, predicate: Expression) -> Result<Self> {
        let input = self.plan;
        let plan = LogicalPlan::Filter(Filter::try_new(Arc::new(input), predicate)?);

        Ok(Self {
            plan,
            optimizer: self.optimizer,
        })
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

        Ok(Self {
            plan,
            optimizer: self.optimizer,
        })
    }

    /// Performs an order_by or sort operation based on
    /// the provided sort expression.
    pub fn order_by(self, expression: Vec<Expression>) -> Self {
        let input = self.plan;
        let plan = LogicalPlan::Sort(Sort::new(Arc::new(input), expression));

        Self {
            plan,
            optimizer: self.optimizer,
        }
    }

    /// Performs a limit operation, skipping rows, and fetching some number of rows.
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Self {
        let input = self.plan;
        let plan = LogicalPlan::Limit(Limit::new(Arc::new(input), skip, fetch));

        Self {
            plan,
            optimizer: self.optimizer,
        }
    }

    pub fn join(
        self,
        rhs: DataFrame,
        join_type: JoinType,
        on_left: &[&str],
        on_right: &[&str],
        filter: Option<Expression>,
    ) -> Self {
        let lhs = Arc::new(self.plan);
        let rhs = Arc::new(rhs.plan);
        let left_keys = on_left.iter().map(|name| col(name.to_string()));
        let right_keys = on_right.iter().map(|name| col(name.to_string()));
        let on = left_keys.zip(right_keys).collect::<Vec<_>>();

        let plan = LogicalPlan::Join(Join::new(lhs, rhs, on, join_type, filter));

        Self {
            plan,
            optimizer: self.optimizer,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty;

    use crate::{
        execution::context::SessionContext,
        expression::logical::expr_fn::{avg, col, count, lit, max, min, sort, sum},
        io::reader::csv::options::CsvReadOptions,
        plan::logical::join::JoinType,
    };

    use super::DataFrame;

    async fn assert_df_results(df: &DataFrame, expected: Vec<&str>) {
        let results = df.collect().await.unwrap();
        let results = pretty::pretty_format_batches(&results).unwrap().to_string();
        let results = results.trim().lines().collect::<Vec<_>>();
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_dataframe_inner_join_no_join_keys_with_filters() {
        let ctx = SessionContext::new();

        let lhs = ctx
            .read_csv("testdata/csv/join_left.csv", CsvReadOptions::new())
            .unwrap();
        let rhs = ctx
            .read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())
            .unwrap();

        let filter = col("l2").gt(lit(1i64));
        let df = lhs.join(rhs, JoinType::Inner, &[], &[], Some(filter));

        let expected = vec![
            "+----+----+----+----+-----+------+",
            "| l1 | l2 | l3 | r1 | r2  | r3   |",
            "+----+----+----+----+-----+------+",
            "| b  | 2  | 20 | a  | 100 | 1000 |",
            "| b  | 2  | 20 | b  | 200 | 2000 |",
            "| b  | 2  | 20 | c  | 300 | 3000 |",
            "| c  | 3  | 30 | a  | 100 | 1000 |",
            "| c  | 3  | 30 | b  | 200 | 2000 |",
            "| c  | 3  | 30 | c  | 300 | 3000 |",
            "| d  | 4  | 40 | a  | 100 | 1000 |",
            "| d  | 4  | 40 | b  | 200 | 2000 |",
            "| d  | 4  | 40 | c  | 300 | 3000 |",
            "| e  | 5  | 50 | a  | 100 | 1000 |",
            "| e  | 5  | 50 | b  | 200 | 2000 |",
            "| e  | 5  | 50 | c  | 300 | 3000 |",
            "| f  | 6  | 60 | a  | 100 | 1000 |",
            "| f  | 6  | 60 | b  | 200 | 2000 |",
            "| f  | 6  | 60 | c  | 300 | 3000 |",
            "+----+----+----+----+-----+------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_inner_join_no_join_keys_no_filters() {
        let ctx = SessionContext::new();

        let lhs = ctx
            .read_csv("testdata/csv/join_left.csv", CsvReadOptions::new())
            .unwrap();
        let rhs = ctx
            .read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())
            .unwrap();

        let df = lhs.join(rhs, JoinType::Inner, &[], &[], None);

        let expected = vec![
            "+----+----+----+----+-----+------+",
            "| l1 | l2 | l3 | r1 | r2  | r3   |",
            "+----+----+----+----+-----+------+",
            "| a  | 1  | 10 | a  | 100 | 1000 |",
            "| a  | 1  | 10 | b  | 200 | 2000 |",
            "| a  | 1  | 10 | c  | 300 | 3000 |",
            "| b  | 2  | 20 | a  | 100 | 1000 |",
            "| b  | 2  | 20 | b  | 200 | 2000 |",
            "| b  | 2  | 20 | c  | 300 | 3000 |",
            "| c  | 3  | 30 | a  | 100 | 1000 |",
            "| c  | 3  | 30 | b  | 200 | 2000 |",
            "| c  | 3  | 30 | c  | 300 | 3000 |",
            "| d  | 4  | 40 | a  | 100 | 1000 |",
            "| d  | 4  | 40 | b  | 200 | 2000 |",
            "| d  | 4  | 40 | c  | 300 | 3000 |",
            "| e  | 5  | 50 | a  | 100 | 1000 |",
            "| e  | 5  | 50 | b  | 200 | 2000 |",
            "| e  | 5  | 50 | c  | 300 | 3000 |",
            "| f  | 6  | 60 | a  | 100 | 1000 |",
            "| f  | 6  | 60 | b  | 200 | 2000 |",
            "| f  | 6  | 60 | c  | 300 | 3000 |",
            "+----+----+----+----+-----+------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_inner_join_with_filters() {
        let ctx = SessionContext::new();

        let lhs = ctx
            .read_csv("testdata/csv/join_left.csv", CsvReadOptions::new())
            .unwrap();
        let rhs = ctx
            .read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())
            .unwrap();

        let filter = col("l2").gt(lit(1i64));
        let df = lhs.join(rhs, JoinType::Inner, &["l1"], &["r1"], Some(filter));

        let expected = vec![
            "+----+----+----+----+-----+------+",
            "| l1 | l2 | l3 | r1 | r2  | r3   |",
            "+----+----+----+----+-----+------+",
            "| b  | 2  | 20 | b  | 200 | 2000 |",
            "| c  | 3  | 30 | c  | 300 | 3000 |",
            "+----+----+----+----+-----+------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_inner_join_no_filters_with_collisions() {
        let ctx = SessionContext::new();

        let lhs = ctx
            .read_csv("testdata/csv/join_left_2.csv", CsvReadOptions::new())
            .unwrap();
        let rhs = ctx
            .read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())
            .unwrap();

        let df = lhs.join(rhs, JoinType::Inner, &["l1"], &["r1"], None);

        let expected = vec![
            "+----+----+----+----+-----+------+",
            "| l1 | l2 | l3 | r1 | r2  | r3   |",
            "+----+----+----+----+-----+------+",
            "| a  | 1  | 10 | a  | 100 | 1000 |",
            "| a  | 1  | 10 | a  | 100 | 1000 |",
            "| b  | 2  | 20 | b  | 200 | 2000 |",
            "| c  | 3  | 30 | c  | 300 | 3000 |",
            "+----+----+----+----+-----+------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_inner_join_no_filters() {
        let ctx = SessionContext::new();

        let lhs = ctx
            .read_csv("testdata/csv/join_left.csv", CsvReadOptions::new())
            .unwrap();
        let rhs = ctx
            .read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())
            .unwrap();

        let df = lhs.join(rhs, JoinType::Inner, &["l1"], &["r1"], None);

        let expected = vec![
            "+----+----+----+----+-----+------+",
            "| l1 | l2 | l3 | r1 | r2  | r3   |",
            "+----+----+----+----+-----+------+",
            "| a  | 1  | 10 | a  | 100 | 1000 |",
            "| b  | 2  | 20 | b  | 200 | 2000 |",
            "| c  | 3  | 30 | c  | 300 | 3000 |",
            "+----+----+----+----+-----+------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_left_join_with_filters() {
        let ctx = SessionContext::new();

        let lhs = ctx
            .read_csv("testdata/csv/join_left.csv", CsvReadOptions::new())
            .unwrap();
        let rhs = ctx
            .read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())
            .unwrap();

        let filter = col("l1").neq(lit("a"));
        let df = lhs.join(rhs, JoinType::Left, &["l1"], &["r1"], Some(filter));

        let expected = vec![
            "+----+----+----+----+-----+------+",
            "| l1 | l2 | l3 | r1 | r2  | r3   |",
            "+----+----+----+----+-----+------+",
            "| b  | 2  | 20 | b  | 200 | 2000 |",
            "| c  | 3  | 30 | c  | 300 | 3000 |",
            "| a  | 1  | 10 |    |     |      |",
            "| d  | 4  | 40 |    |     |      |",
            "| e  | 5  | 50 |    |     |      |",
            "| f  | 6  | 60 |    |     |      |",
            "+----+----+----+----+-----+------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_left_join_no_filters_with_collisions() {
        let ctx = SessionContext::new();

        let lhs = ctx
            .read_csv("testdata/csv/join_left_2.csv", CsvReadOptions::new())
            .unwrap();
        let rhs = ctx
            .read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())
            .unwrap();

        let df = lhs.join(rhs, JoinType::Left, &["l1"], &["r1"], None);

        let expected = vec![
            "+----+----+----+----+-----+------+",
            "| l1 | l2 | l3 | r1 | r2  | r3   |",
            "+----+----+----+----+-----+------+",
            "| a  | 1  | 10 | a  | 100 | 1000 |",
            "| a  | 1  | 10 | a  | 100 | 1000 |",
            "| b  | 2  | 20 | b  | 200 | 2000 |",
            "| c  | 3  | 30 | c  | 300 | 3000 |",
            "| d  | 4  | 40 |    |     |      |",
            "| e  | 5  | 50 |    |     |      |",
            "| f  | 6  | 60 |    |     |      |",
            "+----+----+----+----+-----+------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_left_join_no_filters() {
        let ctx = SessionContext::new();

        let lhs = ctx
            .read_csv("testdata/csv/join_left.csv", CsvReadOptions::new())
            .unwrap();
        let rhs = ctx
            .read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())
            .unwrap();

        let df = lhs.join(rhs, JoinType::Left, &["l1"], &["r1"], None);

        let expected = vec![
            "+----+----+----+----+-----+------+",
            "| l1 | l2 | l3 | r1 | r2  | r3   |",
            "+----+----+----+----+-----+------+",
            "| a  | 1  | 10 | a  | 100 | 1000 |",
            "| b  | 2  | 20 | b  | 200 | 2000 |",
            "| c  | 3  | 30 | c  | 300 | 3000 |",
            "| d  | 4  | 40 |    |     |      |",
            "| e  | 5  | 50 |    |     |      |",
            "| f  | 6  | 60 |    |     |      |",
            "+----+----+----+----+-----+------+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_limit() {
        let ctx = SessionContext::new();

        let df = ctx
            .read_csv("testdata/csv/simple.csv", CsvReadOptions::new())
            .unwrap()
            .limit(1, Some(2));

        let expected = vec![
            "+----+----+----+",
            "| c1 | c2 | c3 |",
            "+----+----+----+",
            "| b  | 2  | 3  |",
            "| c  | 3  | 4  |",
            "+----+----+----+",
        ];
        assert_df_results(&df, expected).await;
    }

    #[tokio::test]
    async fn test_dataframe_order_by() {
        let ctx = SessionContext::new();

        let group_by = vec![col("c1")];
        let aggregate_expressions = vec![count(col("c2")), sum(col("c2"))];
        let order_by = vec![sort(col("SUM(c2)"), false)];

        let df = ctx
            .read_csv("testdata/csv/simple_aggregate.csv", CsvReadOptions::new())
            .unwrap()
            .aggregate(group_by, aggregate_expressions)
            .unwrap()
            .order_by(order_by);

        let expected = vec![
            "+----+-----------+---------+",
            "| c1 | COUNT(c2) | SUM(c2) |",
            "+----+-----------+---------+",
            "| c  | 2         | 8       |",
            "| b  | 1         | 7       |",
            "| f  | 1         | 6       |",
            "| d  | 1         | 4       |",
            "| a  | 2         | 3       |",
            "+----+-----------+---------+",
        ];
        assert_df_results(&df, expected).await;
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

use simple_rust_query_engine::{
    error::Result,
    execution::context::SessionContext,
    expression::logical::expr_fn::{col, count, lit, sum},
    io::reader::csv::options::CsvReadOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let group_by = vec![];
    let aggregate_expressions = vec![count(col("c1")), sum(col("c2"))];

    let df = ctx
        .read_csv("testdata/csv/simple.csv", CsvReadOptions::new())?
        .filter(col("c1").neq(lit("a")))?
        .aggregate(group_by, aggregate_expressions)?;

    // "+-----------+---------+"
    // "| COUNT(c1) | SUM(c2) |"
    // "+-----------+---------+"
    // "| 5         | 20      |"
    // "+-----------+---------+"
    df.show().await
}

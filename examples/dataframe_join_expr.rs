use simple_rust_query_engine::{
    error::Result,
    execution::context::SessionContext,
    expression::logical::expr_fn::{col, lit},
    io::reader::csv::options::CsvReadOptions,
    plan::logical::join::JoinType,
};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let lhs = ctx.read_csv("testdata/csv/join_left.csv", CsvReadOptions::new())?;
    let rhs = ctx.read_csv("testdata/csv/join_right.csv", CsvReadOptions::new())?;

    let filter = col("l2").gt(lit(1));
    let df = lhs.join(rhs, JoinType::Inner, &["l1"], &["r1"], Some(filter))?;

    // +----+----+----+----+-----+------+
    // | l1 | l2 | l3 | r1 | r2  | r3   |
    // +----+----+----+----+-----+------+
    // | b  | 2  | 20 | b  | 200 | 2000 |
    // | c  | 3  | 30 | c  | 300 | 3000 |
    // +----+----+----+----+-----+------+
    df.show().await
}

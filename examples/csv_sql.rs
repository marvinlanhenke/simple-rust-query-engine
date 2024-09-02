use simple_rust_query_engine::{
    error::Result, execution::context::SessionContext, io::reader::csv::options::CsvReadOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv("left", "testdata/csv/join_left.csv", CsvReadOptions::new())?;
    ctx.register_csv(
        "right",
        "testdata/csv/join_right.csv",
        CsvReadOptions::new(),
    )?;

    let df = ctx.sql(
        "
        SELECT \
          l1, SUM(r2), AVG(r3) \
        FROM left \
        LEFT JOIN right ON l1 = r1 \
        WHERE l3 < 40 \
        GROUP BY l1 \
        ORDER BY \"SUM(r2)\" DESC",
    )?;

    // +----+---------+---------+
    // | l1 | SUM(r2) | AVG(r3) |
    // +----+---------+---------+
    // | c  | 300     | 3000.0  |
    // | b  | 200     | 2000.0  |
    // | a  | 100     | 1000.0  |
    // +----+---------+---------+
    df.show().await
}

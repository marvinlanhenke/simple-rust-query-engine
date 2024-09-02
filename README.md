# Simple Rust Query Engine

The Simple Rust Query Engine is an educational project aimed at understanding the fundamentals of query engines by implementing one from scratch in Rust.

Inspired by [How Query Engines Work](https://howqueryengineswork.com/00-introduction.html) and heavily influenced by [DataFusion](https://github.com/apache/datafusion), this project serves as a hands-on approach to learning.

For educational purposes only.

## Background/Motivation

Databases and query engines have always intrigued me, and I've been eager to understand their inner workings. To truly grasp these complex systems, I decided to build a query engine from scratch. This project is a fun challenge and also a potential resource for others interested in the same topic.

Keep in mind that this implementation is not feature-complete, and certain optimizations were deliberately left out to keep the codebase simple and easy to understand.

## High-Level-Overview:

```text
                                           DataFrame
          ┌──────────┐   ┌─────────────┐   ┌───────────────┐    ┌────────────┐
User ────►│   SQL    ├──►│             │   │               │    │            │
Input     │  Parser  │   │  (Logical)  │   │  LogicalPlan  │    │            │
  │       └──────────┘   │             ├──►│     (Tree)    ├───►│            │
  │                      │ Expressions │   │               │    │            │
  └─────────────────────►│             │   │               │    │            │
                         └─────────────┘   └───────────────┘    │            │
                                                                │  Optimizer │
          ┌──────────────┐                 ┌───────────────┐    │            │
          │              │                 │               │    │ (RuleBased)│
          │              │      tokio      │  PhysicalPlan │    │            │
User ◄────┤              │◄────────────────┤     (Tree)    │◄───┤            │
Output    │     Arrow    │     Streams     │               │    │            │
          │  RecordBatch │                 │               │    │            │
          │              │                 └──────┐▲┌──────┘    │            │
          │              │                 ┌──────┘│└──────┐    │            │
          │              │                 │  PhysicalExpr │◄───┤            │
          │              │                 │               │    │            │
          └──────────────┘                 └───────────────┘    └────────────┘
```

## Prerequisites

- Rust (latest stable version recommended)
- Basic understanding of Rust programming
- Familiarity with SQL and database concepts

## Installation

To get started with the Simple Rust Query Engine, follow these steps:

1. **Clone the repository:**

```shell
git clone https://github.com/marvinlanhenke/simple-rust-query-engine
cd simple-rust-query-engine
```

2. **Build the project:**

```shell
cargo build
```

3. **Run examples:**

```shell
cargo run --example dataframe_join_expr
cargo run --example csv_sql
```

## Features:

- **SQL support:** Basic SQL query support with limited functionality.
- **CSV support:** Ability to read and query CSV files.
- **DataFrame Expression API:** Chainable API for building complex queries.
- **Aggregations, GroupBy, Joins:** Support for common SQL operations like aggregates, group-by, joins, etc.
- **Rule-based Query Optimizer:** Simple rule-based optimization, including predicate pushdown and projection pushdown.
- **Type Coercion:** Automatic type conversion in expressions.

## Examples:

Here are some example usages of the Simple Rust Query Engine.

#### Dataframe Expression API

```Rust
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

```

#### SQL API

```Rust
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
```

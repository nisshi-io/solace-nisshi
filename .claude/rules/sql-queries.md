---
paths:
  - "nisshi-storage-sql/**"
---

## SQL Queries

The PostgreSQL, libSQL and Turso engines run the same queries, from `nisshi-storage-sql/src/sql/`.
An engine that reads a column by a hand-written index reads the wrong column when the query's
`select` list changes, and nothing fails. An engine therefore reads each column by its name,
through code that the query's module declares once for all three drivers.

The examples below are illustrative, not taken from a real file, so they stay true when the code
changes.

## Rules

- **Give each query that returns rows a module beside it.** `sql/topic_select.sql` gets
  `sql/topic_select.rs`, declared in `sql.rs`. The module holds a `QUERY` constant, a `Query` of
  the module's row with the query's name in the SQL cache, and declares one row of the result with
  `sql_row!`, one line for each column:

  ```rust
  /// The query's name in the SQL cache.
  pub(crate) const QUERY: Query<Row> = Query::new("topic_select.sql");

  sql_row! {
      /// One topic of the cluster.
      #[derive(Clone, Debug, Eq, PartialEq)]
      pub(crate) struct Row {
          /// The topic's name.
          name: String,
          /// The topic's partition count.
          partitions: i32,
      }
  }
  ```

  A query whose engines read no column, because a row only means that something exists, needs
  only a `QUERY` that holds its name, as a `&str`. Variants of one query, such as a PostgreSQL
  version and a SQLite version, are constants in one module.
- **Name a field as its column, and rename a column in the SQL.** When the Rust name should differ
  from a table's column, the query renames the column with `as`, as in `p.id as producer_id`.
- **Share one row type between the queries that return the same thing.** Declare it once, in an
  inline module of `sql/row.rs` named for what one row holds, and re-export it from each query's
  module with `pub(crate) use super::row::topic::Row;`. A row that only one query returns stays in
  that query's module.
- **Read every row through the engine's helper.** The helper takes the query's `QUERY` and
  returns rows of its row type, so a call site can't pair a query with another query's row. Never
  write the query's file name, a column's index or a `RowReader` at a call site:

  ```rust
  let mut rows = connection.query_rows(topic_select::QUERY, params).await?;

  while let Some(topic_select::Row { name, partitions }) = rows.next().await? {
      // ...
  }
  ```

  Use a one-row helper for a result of at most one row. Build a reader yourself only to read one
  result as two row types, as PostgreSQL's fetch does through `prepare_query_result`.
- **Write the row type qualified, as `topic_select::Row`.** Each engine file also imports its
  driver's own `Row`, and the qualified name shows which of the two a line means.
- **Declare each field as the driver reads it, and decode it in the engine.** A field's type
  implements `FromColumn` in `sql/driver.rs`. A JSON column is
  `serde_json::Result<serde_json::Value>`, so the engine decides what an undecodable document does
  to the rest of the result. On libSQL and Turso, a `SystemTime` field reads integer milliseconds,
  and a column with a `current_timestamp` default holds text there. A new kind of column gets one
  `FromColumn` implementation, and a new kind of stored value gets one `DriverRow` method for each
  driver.
- **Name the engines that run a query on its line in `queries!`,** as `[Postgres, Libsql, Turso]`.
  Each engine's `every_row_query_returns_the_columns_of_its_row` test checks those queries against
  their rows.
- **Put what a row means in the query's module.** When a result has a row that is not data, such
  as a marker row for a topic that sets none of the requested keys, the module gives `Row` a method
  that returns `None` for it, and each engine calls that method instead of testing the field.
- **Name every column that an engine reads with `as`, unless it is a plain column.** PostgreSQL
  names `count(*)` `count`, and SQLite names it by the expression's text. `count(*) as partitions`
  gives it one name everywhere. This applies to `returning` lists too.
- **Do not write `select *` in a query that an engine reads.** Its column names come from the
  tables, and a change to a table changes the result without a change to the query.
- **Give each column of a result a unique, lower-case name.** PostgreSQL folds an unquoted name to
  lower case and SQLite does not. Do not quote a column name.

## The Test

Change the order of a query's `select` list. Every engine still reads the same values. Rename a
column in the query. Every engine returns `Error::MissingColumn`, and the
`every_row_query_returns_the_columns_of_its_row` tests fail.

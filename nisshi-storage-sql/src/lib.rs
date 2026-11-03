// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![deny(clippy::todo, clippy::unimplemented)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::todo,
        clippy::unimplemented
    )
)]

#[cfg(any(feature = "libsql", feature = "postgres"))]
mod schema;
mod sql;

#[cfg(any(feature = "libsql", feature = "turso"))]
mod database_file;

#[cfg(feature = "libsql")]
pub mod lite;

#[cfg(feature = "turso")]
pub mod limbo;

#[cfg(feature = "postgres")]
pub mod pg;

#[cfg(feature = "libsql")]
pub use lite::factory::LiteFactory as LiteEngineFactory;

#[cfg(feature = "turso")]
pub use limbo::factory::LimboFactory as LimboEngineFactory;

#[cfg(feature = "postgres")]
pub use pg::factory::PostgresFactory as PostgresEngineFactory;

/// Returns the PostgreSQL query that locks a partition's watermark row before a write, as the
/// engine runs it, so that a test can hold the lock from its own connection while a produce runs.
#[cfg(all(feature = "test-support", feature = "postgres"))]
#[doc(hidden)]
pub fn watermark_select_for_write() -> nisshi_storage::Result<&'static str> {
    sql::SQL.get(sql::watermark_select_for_write::QUERY.key())
}

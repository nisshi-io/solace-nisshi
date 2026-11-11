-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Returns whether a namespace on the search path holds the nisshi_schema_version table.
--
-- This query asks the database directly, by reading pg_class, which always shows every table
-- committed before the query started. It doesn't call to_regclass, because to_regclass answers
-- from the connection's cache of table names, and that cache can be out of date.
--
-- The broker runs this query twice: before it asks for the setup lock, and again once it holds it.
-- If another broker held the lock, that broker has created the table in between. The cache can
-- still hold the first answer, that the table doesn't exist, and the broker would then create the
-- schema a second time.

select exists (
    select
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where c.relname = 'nisshi_schema_version'
    and n.nspname = any (current_schemas(false))
) as version_table_exists

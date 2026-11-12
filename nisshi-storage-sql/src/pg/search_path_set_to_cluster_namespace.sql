-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ Solace Corporation
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

-- Puts only the namespace that holds the `cluster` table on the search path, for the current
-- transaction. PostgreSQL calls a namespace a schema. Returns no row, and changes nothing, in an
-- empty database.

select set_config('search_path', quote_ident(n.nspname), true)

from

pg_class c
join pg_namespace n on n.oid = c.relnamespace

where

c.oid = to_regclass('cluster')

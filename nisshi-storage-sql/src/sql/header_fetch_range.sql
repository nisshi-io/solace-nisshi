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

-- The headers of the records that a fetch returns, in one query. The offsets are those of the
-- returned records, so the cost follows the response: a keyed fetch returns a sparse set of
-- offsets from a wide range, and the headers of the records in between are not read.
--
-- As in record_fetch.sql, the topition id comes from a scalar subquery and not from a join: a join
-- hides from the planner that the rows come out in offset order, and it sorts them, which for the
-- headers of 10000 records spills to a temp file.
select h.offset_id, h.k as key, h.v as value
from header h
where h.topition = (
    select tp.id
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    where c.name = $1
    and t.name = $2
    and tp.partition = $3)
and h.offset_id = any($4)
order by h.offset_id, h.ordinal;

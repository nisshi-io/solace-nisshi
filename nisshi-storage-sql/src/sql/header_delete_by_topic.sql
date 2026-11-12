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

delete from header
where header.topition in (
    select r.topition
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join record r on r.topition = tp.id
    where c.name = $1
    and t.name = $2
)

and

header.offset_id in (
    select r.offset_id
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join record r on r.topition = tp.id
    where c.name = $1
    and t.name = $2
);

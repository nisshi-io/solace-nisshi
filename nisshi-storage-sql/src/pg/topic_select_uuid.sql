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

select

t.uuid as uuid,
t.name as name,
t.is_internal as is_internal,
t.partitions as partitions,
t.replication_factor as replication_factor

from

cluster c
join topic t on t.cluster = c.id

where

c.name = $1
and t.uuid = $2

union

select

vt.uuid as uuid,
t.name || '/' || convert_from(vt.k, 'UTF-8') as name,
t.is_internal as is_internal,
t.partitions as partitions,
t.replication_factor as replication_factor

from
cluster c
join topic t on t.cluster = c.id
join virtual_topic vt on vt.topic = t.id

where

c.name = $1
and vt.uuid = $2

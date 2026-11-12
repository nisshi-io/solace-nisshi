-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

select h.k as key, h.v as value
from header h
join record r on h.topition = r.topition and h.offset_id = r.offset_id
join topition tp on tp.id = r.topition
join topic t on tp.topic = t.id
join cluster c on t.cluster = c.id
where c.name = $1
and t.name = $2
and tp.partition = $3
and r.offset_id = $4
order by h.ordinal;

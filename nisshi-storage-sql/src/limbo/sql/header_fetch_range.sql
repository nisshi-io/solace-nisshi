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


-- The headers of the records from offset $4 to offset $5, both included, in the order of their
-- records and then of their ordinals. A fetch runs it once, for the records that it returns,
-- instead of once for each record.

select
h.offset_id,
h.k,
h.v

from header h

where h.topition = (
    select tp.id
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    where c.name = $1
    and t.name = $2
    and tp.partition = $3)
and h.offset_id >= $4
and h.offset_id <= $5

order by h.offset_id, h.ordinal;

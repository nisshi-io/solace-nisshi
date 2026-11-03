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


-- Reads the records of one partition from an offset up to the high watermark, in offset order,
-- at most $6 of them. It streams: the first row comes out as soon as it is read, and the work is
-- in proportion to the rows that the caller reads, not to the size of the backlog.
--
-- The shared `record_fetch.sql` sums the sizes of the records with a window function over the whole
-- range, and filters on the sum afterwards. Turso evaluates the window before it returns a row, so
-- a fetch from the start of a long backlog took longer than `max_wait` to answer its first record.
-- The caller adds up the sizes and stops at the byte limit.
--
-- The partition is a scalar subquery, not a join. With a join Turso sorts the whole range before
-- it applies the limit, however small the limit is.

select
r.offset_id,
r.attributes,
r.timestamp,
r.k,
r.v,
r.producer_id,
r.producer_epoch

from record r

where r.topition = (
    select tp.id
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    where c.name = $1
    and t.name = $2
    and tp.partition = $3)
and r.offset_id >= $4
and r.offset_id < $5

order by r.offset_id
limit $6;

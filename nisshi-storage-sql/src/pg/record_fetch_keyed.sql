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

-- prepare record_fetch_keyed (text, text, integer, integer, integer, integer, bytea, bigint) as
-- A fetch reads at most $8 rows past the fetch offset (ROWS_PER_FETCH in pg.rs, 10000). The running byte total below is
-- computed over every row that this query reads, so without the limit, one fetch costs as much
-- as the whole backlog between the fetch offset and the high watermark, however few rows the
-- client can take. The fetch service reads again from the last offset that it received, so a
-- limit that ends a read early costs one more query.
--
-- The record table is read directly, with the topition id looked up in a scalar subquery. A join
-- between the two hides from the planner that the rows come out in offset order, and it then sorts
-- all of them, which for 10000 rows of 1 KiB spills to a temp file. Without the join, the limit
-- stops an ordered walk of the primary key, and the running byte total reads that order as it is.
--
-- The fetch runs with the settings in FETCH_PLAN_SETTINGS (pg.rs), which turn off bitmap and
-- sequential scans, so that this query keeps the ordered scan that the limit relies on. Run it
-- with them to see the plan that a fetch uses: `run.sh explain` does.
with candidates as (
select

r.offset_id,
r.attributes,
r.timestamp,
r.k,
r.v,
r.producer_id,
r.producer_epoch

from

record r

where

r.topition = (
    select tp.id
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    where c.name = $1
    and t.name = $2
    and tp.partition = $3)
and r.offset_id >= $4
and r.offset_id < $6
and r.k = convert_to($7, 'UTF-8')

order by r.offset_id
limit $8),

sized as (
select

offset_id,
attributes,
timestamp,
k,
v,
sum(coalesce(length(k), 0) + coalesce(length(v), 0)) over (order by offset_id) as bytes,
row_number() over (order by offset_id) as ordinal,
producer_id,
producer_epoch

from

candidates)

-- The query returns the first row even when that row alone is larger than $5, so that a consumer
-- always gets past a record larger than its fetch limit (KIP-74). Kafka returns the whole first
-- batch, and only for the first non-empty partition of a fetch ([ReplicaManager.scala#L1733-L1739],
-- [LogSegment.java#L439-L441]). This query returns the first record of every read, because it
-- does not know about batches or about the other partitions of the fetch.
--
-- The query finds the first row by its ordinal, not by a running total equal to its own size,
-- because a row with a null key and a null value has a size of 0. It does not compare offset_id
-- with a min(offset_id) over sized, because a second reference to sized makes PostgreSQL store
-- every row of it, keys and values included, instead of streaming them.
--
-- [ReplicaManager.scala#L1733-L1739]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733-L1739
-- [LogSegment.java#L439-L441]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L439-L441
select offset_id, attributes, timestamp, k as key, v as value, bytes, producer_id, producer_epoch
from sized
where bytes < $5
or ordinal = 1
order by offset_id;

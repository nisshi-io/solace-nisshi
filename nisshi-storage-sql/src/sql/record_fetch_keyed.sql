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

-- prepare record_fetch_keyed (text, text, integer, integer, integer, integer, blob, integer) as
-- A fetch reads at most $8 rows past the fetch offset (ROWS_PER_FETCH in sql.rs, 10000), as the
-- Postgres statements do. The running byte total below is computed over every row that this query
-- reads, so without the limit, one fetch costs as much as the whole backlog between the fetch
-- offset and the high watermark, however few rows the client can take, and builds every one of them
-- in memory. With the limit, a read holds at most that many rows, so its memory is bounded by
-- the rows times the size of a record, and by max_bytes only up to that. The fetch service reads again from the last
-- offset that it received, so a limit that ends a read early costs one more query.
--
-- The limit counts the rows that match the key, not the rows that the read passes over. A read that
-- stopped after a number of rows passed over could return no rows while more remain, and the fetch
-- service takes an empty read to mean that the partition is caught up. The cost is that a read for a
-- rare key still scans the non-matching rows until it finds enough matches (SOL-157341).
--
-- A record's size is its key, its value and its headers, as Kafka counts the whole record batch
-- toward max_bytes, and each header costs 2 bytes more, the smallest that it takes on the wire.
-- Every record also costs 7 bytes, the smallest record that Kafka writes: a length, attributes, two
-- deltas, a null key, a null value and no headers, 1 byte each. That is less than Kafka counts,
-- which adds 61 bytes for the header of each record batch
-- ([DefaultRecordBatch.java#L129]), because this query does not know where the batches that
-- a read builds begin. Counting less than Kafka means a read spends the budget no sooner than Kafka would for the
-- same records, and a record with no key, no value and no headers no longer costs 0. The bytes of
-- a response can therefore exceed max_bytes by the framing that this count leaves out.
-- The header table is probed by its primary key for each record, so a record that holds only
-- headers still spends the budget. The probe is a result column of a derived table, because the
-- Turso engine does not accept a subquery inside the argument of a window function.
--
-- [DefaultRecordBatch.java#L129]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L129
with candidates as (
select

r.topition,
r.offset_id,
r.attributes,
r.timestamp,
r.k,
r.v,
r.producer_id,
r.producer_epoch

from

cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join record r on r.topition = tp.id

where

c.name = $1
and t.name = $2
and tp.partition = $3
and r.offset_id >= $4
and r.offset_id < $6
and r.k = $7

order by r.offset_id
limit $8),

sized as (
select

offset_id,
attributes,
timestamp,
k,
v,
sum(row_bytes) over (order by offset_id) as bytes,
row_number() over (order by offset_id) as ordinal,
producer_id,
producer_epoch

from (

select

c.offset_id,
c.attributes,
c.timestamp,
c.k,
c.v,
7
    + coalesce(length(c.k), 0)
    + coalesce(length(c.v), 0)
    + (select coalesce(sum(coalesce(length(h.k), 0) + coalesce(length(h.v), 0) + 2), 0)
       from header h
       where h.topition = c.topition
       and h.offset_id = c.offset_id) as row_bytes,
c.producer_id,
c.producer_epoch

from

candidates c) sized_rows)

-- The query returns the first row even when that row alone is larger than $5, so that a consumer
-- always gets past a record larger than its fetch limit (KIP-74). Kafka returns the whole first
-- batch, and only for the first non-empty partition of a fetch ([ReplicaManager.scala#L1733-L1739],
-- [LogSegment.java#L439-L441]). This query returns the first record of every read, because it
-- does not know about batches or about the other partitions of the fetch. The query finds the
-- first row by its ordinal, not by a running total equal to its own size, because a later
-- row can have the same size as the first.
--
-- [ReplicaManager.scala#L1733-L1739]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733-L1739
-- [LogSegment.java#L439-L441]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L439-L441
select offset_id, attributes, timestamp, k, v, bytes, producer_id, producer_epoch
from sized
where bytes < $5
or ordinal = 1
order by offset_id;

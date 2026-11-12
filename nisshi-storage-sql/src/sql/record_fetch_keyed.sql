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
-- A fetch reads at most $8 rows past the fetch offset (ROWS_PER_FETCH). The running byte total
-- below covers every row that this query reads, so without the limit, one fetch costs as much as
-- the whole backlog between the fetch offset and the high watermark, however few rows the client
-- can take. The limit also bounds the header probes, which run once for each row that the limit
-- keeps. The fetch service reads again from the last offset that it received, so a limit that
-- ends a read early costs one more query.
--
-- A record's size is its key, its value and its headers, as Kafka counts the whole record batch
-- toward max_bytes, and each header costs 2 bytes more, the smallest that it takes on the wire.
-- Every record also costs 7 bytes, the smallest record that Kafka writes: a length, attributes, two
-- deltas, a null key, a null value and no headers, 1 byte each. That is less than Kafka counts,
-- which adds 61 bytes for the header of each record batch
-- ([DefaultRecordBatch.java#L129]), because this query does not know where the batches that
-- a read builds begin. Counting less than Kafka means a read spends the budget no sooner than Kafka
-- would for the same records, and a record with no key, no value and no headers still costs 7.
-- The bytes of a response can therefore exceed max_bytes by the framing that this count leaves out.
-- The header table is probed by its primary key for each record, so a record that holds only
-- headers still spends the budget. The probe is a result column of a derived table, because the
-- Turso engine does not accept a subquery inside the argument of a window function.
--
-- The window carries the size of each record, not its key or its value. SQLite copies each input
-- row of a window into a temporary table, and the window reads every record from $4 to $6, up to
-- the limit of $8, because the filter on the running total runs after it. A key and a value in the
-- window make one fetch copy the payload of the whole backlog, however few records the fetch
-- returns. SQLite's length() reads only the record header, so the window reads sizes, and the final
-- select reads the key and the value, by primary key, of only the records that it returns. The
-- Turso engine reads each value to compute its length, so on Turso a fetch still reads the payload
-- of every record that the limit keeps.
--
-- The filter on r.k reads the payload on SQLite too. The record table declares v before k, so
-- SQLite walks the overflow pages of each value to reach the key. A keyed fetch therefore still
-- reads the payload of every record that it walks to find the limit's rows of that key, though it
-- no longer copies it into the window. The fix needs a schema change: k before v, a stored hash of
-- the key, declared before v or indexed (an ALTER TABLE ADD COLUMN appends it after v, and the
-- filter still walks v), or an index on (topition, k, offset_id).
--
-- [DefaultRecordBatch.java#L129]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L129
with sized as (
select

topition,
offset_id,
sum(row_bytes) over (order by offset_id) as bytes,
row_number() over (order by offset_id) as ordinal

from (

select

r.topition,
r.offset_id,
7
    + coalesce(length(r.k), 0)
    + coalesce(length(r.v), 0)
    + (select coalesce(sum(coalesce(length(h.k), 0) + coalesce(length(h.v), 0) + 2), 0)
       from header h
       where h.topition = r.topition
       and h.offset_id = r.offset_id) as row_bytes

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
limit $8) sized_rows)

-- The query returns the first row even when that row alone is larger than $5, so that a consumer
-- always gets past a record larger than its fetch limit (KIP-74). Kafka returns the whole first
-- batch, and only for the first non-empty partition of a fetch ([ReplicaManager.scala#L1733-L1739],
-- [LogSegment.java#L439-L441]). This query returns the first record of every read, because it
-- does not know about batches or about the other partitions of the fetch. The query finds the
-- first row by its ordinal, so that finding it does not depend on every record having a
-- non-zero size.
--
-- [ReplicaManager.scala#L1733-L1739]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733-L1739
-- [LogSegment.java#L439-L441]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L439-L441
select

r.offset_id,
r.attributes,
r.timestamp,
r.k as key,
r.v as value,
s.bytes,
r.producer_id,
r.producer_epoch

from

sized s
join record r on r.topition = s.topition and r.offset_id = s.offset_id

where

s.bytes < $5
or s.ordinal = 1

-- The rows must come out in offset order, because the engine builds record batches from them
-- in the order that it reads them, and each record's offset delta counts from the first record
-- of its batch. A window does not guarantee the order of the rows that it returns.
order by r.offset_id;

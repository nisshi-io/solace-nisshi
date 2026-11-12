-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ Solace Corporation
--
-- Contributors outside Solace:
--   Or Hayat
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

-- prepare record_fetch (text, text, integer, integer, integer, integer, bigint) as
-- Each row holds one record with all of its headers, so that a read stopped after any row keeps
-- only complete records.
--
-- The fetch runs with FETCH_PLAN_SETTINGS (pg.rs), which make PostgreSQL read the record and
-- header tables in offset order and merge-join the records with their headers. The merge join
-- sends its first row as soon as the first record is read, and stops reading headers one row past
-- the last record. The test fetch_query_plans_stream_records_with_headers (pg.rs) checks that
-- plan, and `benchmark/openmessaging/run.sh explain` shows it.
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
--
-- [DefaultRecordBatch.java#L129]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L129
--
-- The header table is probed by its primary key for each candidate, so a record that holds only
-- headers still spends the budget. The rows that the read returns stop at $5 bytes, but the
-- read still covers up to $7 rows, and probes the header table once for each.
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

record r

where

-- The query looks up the topition id in scalar subqueries, not in a join. The planner then knows
-- that the scan reads one topition from the primary key (topition, offset_id), so the rows come
-- out in offset order and the limit stops the scan early. With a join, the planner sorts every
-- row in the offset range first, and FETCH_PLAN_SETTINGS also forces any join into a merge join.
r.topition = (
    select tp.id
    from topition tp
    where tp.topic = (
        select t.id
        from topic t
        where t.cluster = (select c.id from cluster c where c.name = $1)
        and t.name = $2)
    and tp.partition = $3)
and r.offset_id >= $4
and r.offset_id < $6

-- The limit stops the scan after $7 rows (ROWS_PER_FETCH in sql.rs). The byte limit at the end of
-- the query does not stop it, because PostgreSQL does not know that the running total only grows,
-- so without this limit the scan reads every row up to the high watermark.
order by r.offset_id
limit $7),

sized as (
select

offset_id,
attributes,
timestamp,
k,
v,
(sum(
    7
    + coalesce(length(k), 0)
    + coalesce(length(v), 0)
    + (select coalesce(sum(coalesce(length(h.k), 0) + coalesce(length(h.v), 0) + 2), 0)
       from header h
       where h.topition = candidates.topition
       and h.offset_id = candidates.offset_id)
) over (order by offset_id))::bigint as bytes,
row_number() over (order by offset_id) as ordinal,
producer_id,
producer_epoch

from

candidates),

-- The headers of each record, in produce order. A record without headers has no row here.
headers as (
select

h.offset_id,
array_agg(h.k order by h.ordinal) as header_keys,
array_agg(h.v order by h.ordinal) as header_values

from

header h

where

h.topition = (
    select tp.id
    from topition tp
    where tp.topic = (
        select t.id
        from topic t
        where t.cluster = (select c.id from cluster c where c.name = $1)
        and t.name = $2)
    and tp.partition = $3)
and h.offset_id >= $4
and h.offset_id < $6

group by h.offset_id)

-- The query returns the first row even when that row alone is larger than $5, so that a consumer
-- always gets past a record larger than its fetch limit (KIP-74). Kafka returns the whole first
-- batch, and only for the first non-empty partition of a fetch ([ReplicaManager.scala#L1733-L1739],
-- [LogSegment.java#L439-L441]). This query returns the first record of every read, because it
-- does not know about batches or about the other partitions of the fetch.
--
-- The query finds the first row by its ordinal, so that finding it does not depend on every
-- record having a non-zero size. It does not compare offset_id
-- with a min(offset_id) over sized, because a second reference to sized makes PostgreSQL store
-- every row of it, keys and values included, instead of streaming them.
--
-- [ReplicaManager.scala#L1733-L1739]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733-L1739
-- [LogSegment.java#L439-L441]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L439-L441
select

s.offset_id,
s.attributes,
s.timestamp,
s.k as key,
s.v as value,
s.producer_id,
s.producer_epoch,
h.header_keys,
h.header_values

from

sized s
left join headers h on h.offset_id = s.offset_id

where

s.bytes < $5
or s.ordinal = 1

order by s.offset_id;

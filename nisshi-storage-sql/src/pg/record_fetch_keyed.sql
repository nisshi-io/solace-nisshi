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
-- Each row holds one record with all of its headers, so that a read stopped after any row keeps
-- only complete records.
--
-- The headers of each candidate come from one lookup on the header table's primary key, not from
-- a join. The header table does not hold the record's key, so a join would read the headers of
-- every record between the keyed ones. The fetch runs with FETCH_PLAN_SETTINGS (pg.rs),
-- which make PostgreSQL read the record table in offset order, so the query sends its first row as
-- soon as the first keyed record is read. The test fetch_query_plans_stream_records_with_headers
-- (pg.rs) checks that plan, and `benchmark/openmessaging/run.sh explain` shows it.
--
-- A record's size is its key, its value and its headers, as Kafka counts the whole record batch
-- toward max_bytes, and each header costs 2 bytes more, the smallest that it takes on the wire.
-- The query sizes each candidate from the headers that its lookup returns, so a record that holds
-- only headers still spends the budget. The query returns rows only until they reach $5 bytes, but
-- it looks up the headers of every candidate, up to $8 of them, and reads each header value in
-- full, including the large values that PostgreSQL stores out of line. Summing length() for each
-- candidate, and looking up headers only for the returned rows, would read fewer values, but it
-- measured slower when many records match the key, and no faster when few do.
with candidates as (
select

r.topition,
r.offset_id,
r.attributes,
r.timestamp,
r.k,
r.v,
r.producer_id,
r.producer_epoch,

-- The record's header keys in produce order, followed by its header values in the same order, from
-- one lookup on the header table's primary key. Null for a record without headers.
(select array_agg(h.k order by h.ordinal) || array_agg(h.v order by h.ordinal)
 from header h
 where h.topition = r.topition
 and h.offset_id = r.offset_id) as headers

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
and r.k = convert_to($7, 'UTF-8')

-- The limit stops the scan after $8 rows (ROWS_PER_FETCH in pg.rs). The byte limit at the end of
-- the query does not stop it, because PostgreSQL does not know that the running total only grows,
-- so without this limit the scan reads every row up to the high watermark.
order by r.offset_id
limit $8),

sized as (
select

offset_id,
attributes,
timestamp,
k,
v,
(sum(
    coalesce(length(k), 0)
    + coalesce(length(v), 0)
    + coalesce((select sum(coalesce(length(e), 0)) from unnest(headers) as e), 0)
    + coalesce(cardinality(headers), 0)
) over (order by offset_id))::bigint as bytes,
row_number() over (order by offset_id) as ordinal,
producer_id,
producer_epoch,
headers

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
select

s.offset_id,
s.attributes,
s.timestamp,
s.k,
s.v,
s.producer_id,
s.producer_epoch,

-- The first half of headers holds the keys, and the second half the values.
s.headers[1:cardinality(s.headers) / 2] as header_keys,
s.headers[cardinality(s.headers) / 2 + 1:cardinality(s.headers)] as header_values

from

sized s

where

s.bytes < $5
or s.ordinal = 1

order by s.offset_id;

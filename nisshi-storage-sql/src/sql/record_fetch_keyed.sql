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

-- prepare record_fetch_keyed (text, text, integer, integer, integer, integer, blob) as
with sized as (
select

r.offset_id,
r.attributes,
r.timestamp,
r.k,
r.v,
sum(coalesce(length(r.k), 0) + coalesce(length(r.v), 0)) over (order by r.offset_id) as bytes,
row_number() over (order by r.offset_id) as ordinal,
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
and r.k = $7)

-- The query returns the first row even when that row alone is larger than $5, so that a consumer
-- always gets past a record larger than its fetch limit (KIP-74). Kafka returns the whole first
-- batch, and only for the first non-empty partition of a fetch ([ReplicaManager.scala#L1733-L1739],
-- [LogSegment.java#L439-L441]). This query returns the first record of every read, because it
-- does not know about batches or about the other partitions of the fetch. The query finds the
-- first row by its ordinal, not by a running total equal to its own size, because a row with a
-- null key and a null value has a size of 0.
--
-- [ReplicaManager.scala#L1733-L1739]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733-L1739
-- [LogSegment.java#L439-L441]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L439-L441
select offset_id, attributes, timestamp, k as key, v as value, bytes, producer_id, producer_epoch
from sized
where bytes < $5
or ordinal = 1;

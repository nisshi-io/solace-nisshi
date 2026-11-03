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

-- prepare watermark_select (text, text, integer) as

with stable as (

select

t.id as topic, tp.id as topition, min(txn_po.offset_start) as offset

from

cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn on txn.cluster = c.id
join txn_detail txn_d on txn_d."transaction" = txn.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id and txn_tp.topition = tp.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id

where

c.name = $1
and t.name = $2
and tp.partition = $3
and (txn_d.status = 'PREPARE_COMMIT' or txn_d.status = 'PREPARE_ABORT' or txn_d.status = 'BEGIN')

group by t.id, tp.id

)

-- The last stable offset is the first offset of the oldest open transaction,
-- and null when no transaction is open. DeleteRecords can move the log start
-- (watermark.low) past that offset. Kafka then raises the last stable offset
-- to the log start, so it never answers an offset below the log:
-- https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L964-L978
-- CASE, not greatest(), because libSQL and Turso run this statement too.
select
w.low,
w.high,
case
    when s.offset < coalesce(w.low, 0) then coalesce(w.low, 0)
    else s.offset
end as stable

from

cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join watermark w on w.topition = tp.id
left join stable s on s.topic = t.id and s.topition = tp.id

where c.name = $1
and t.name = $2
and tp.partition = $3;

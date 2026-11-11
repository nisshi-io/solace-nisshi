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

-- prepare list_latest_offset (text, text, integer) as

-- Kafka answers read_committed Latest with the last stable offset: the first
-- offset of the oldest transaction still open on the partition, or the high
-- watermark when no transaction is open. Kafka raises the last stable offset
-- to the log start when it is below it, so that Latest is never below
-- Earliest. Retention and DeleteRecords can delete the records of an open
-- transaction, and the query then answers the offset that Earliest answers.

with

requested_topition as (
    select tp.id, w.low, w.high
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join watermark w on w.topition = tp.id
    where c.name = $1
    and t.name = $2
    and tp.partition = $3
),

-- The offset that list_earliest_offset.sql answers: the first record at or
-- above watermark.low, or watermark.high when no such record exists.
earliest as (
    select coalesce(
        (select min(r.offset_id)
        from record r
        where r.topition = requested_topition.id
        and r.offset_id >= coalesce(requested_topition.low, 0)),
        requested_topition.high,
        0) as offset_id
    from requested_topition
),

-- The aggregate gives one row, with a null offset_start when no transaction is open.
oldest_open_transaction as (
    select min(txn_po.offset_start) as offset_start
    from requested_topition
    join txn_topition txn_tp on txn_tp.topition = requested_topition.id
    join txn_detail txn_d on txn_d.id = txn_tp.txn_detail
    join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
    where txn_d.status in ('BEGIN', 'PREPARE_COMMIT', 'PREPARE_ABORT')
),

last_stable as (
    select
    case
        when oldest_open_transaction.offset_start is null then null
        when oldest_open_transaction.offset_start < earliest.offset_id
            then earliest.offset_id
        else oldest_open_transaction.offset_start
    end as offset_id
    from earliest
    cross join oldest_open_transaction
)

select

coalesce(last_stable.offset_id, requested_topition.high, 0) as offset_id,
r.timestamp

from

requested_topition
cross join last_stable
-- The query also answers the timestamp of a record: the record at the last
-- stable offset, or the newest record. Retention can delete that record, so
-- the left join keeps the row, and the timestamp is then null.
left join record r on r.topition = requested_topition.id
    and r.offset_id = coalesce(
        last_stable.offset_id,
        requested_topition.high - 1);

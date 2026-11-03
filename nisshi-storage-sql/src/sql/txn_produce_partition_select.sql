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

-- prepare txn_produce_partition_select (text, text, bigint, smallint, text, integer) as

select

txn.producer as producer,

coalesce((
    select max(case when txn_d.status in ('PREPARE_COMMIT', 'PREPARE_ABORT') then 2 else 1 end)
    from producer_epoch pe
    join txn_detail txn_d
    on txn_d."transaction" = txn.id
    and txn_d.producer_epoch = pe.id
    join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id
    join topition tp on tp.id = txn_tp.topition
    join topic t on t.id = tp.topic
    where pe.producer = $3
    and pe.epoch = $4
    and t.cluster = c.id
    and t.name = $5
    and tp.partition = $6
), 0) as added

from

cluster c
join txn on txn.cluster = c.id

where

c.name = $1
and txn.name = $2;

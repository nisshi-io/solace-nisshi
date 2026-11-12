-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ Solace Corporation
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

insert into txn_produce_offset
(txn_topition, offset_start, offset_end)

select txn_tp.id, $7, $8

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id and txn_tp.topition = tp.id

where

c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4
and t.name = $5
and tp.partition = $6

on conflict (txn_topition)

do update

set offset_end = excluded.offset_end,
last_updated = excluded.last_updated;

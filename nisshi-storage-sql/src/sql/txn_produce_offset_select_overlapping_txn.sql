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

-- prepare txn_produce_offset_select_overlapping_txn (text, text, integer, integer, text, integer, integer) as

select txn.name, p.id as producer_id, pe.epoch as producer_epoch, txn_d.status, txn_po.offset_start, txn_po.offset_end

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id and txn_tp.topition = tp.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id

where

c.name = $1
and t.name = $5
and tp.partition = $6
and txn_po.offset_start < $7
and txn_d.status is not null

except

select txn.name, p.id as producer_id, pe.epoch as producer_epoch, txn_d.status, txn_po.offset_start, txn_po.offset_end

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id and txn_tp.topition = tp.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id

where

c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4
and t.name = $5
and tp.partition = $6

order by offset_end asc;

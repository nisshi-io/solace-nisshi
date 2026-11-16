-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright (c) Solace Corporation
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

select

t.name as topic,
tp.partition as partition

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join topic t on t.cluster = c.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
join topition tp on tp.topic = t.id and txn_tp.topition = tp.id

where

c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4

-- Deterministic order is required, not cosmetic: end_in_tx's caller takes a per-partition
-- row lock (watermark FOR NO KEY UPDATE, via produce_in_tx) for each row this query
-- returns, held for the rest of that DB transaction. Two transactions touching the same
-- partitions, added via AddPartitionsToTxn in different orders, finalizing concurrently,
-- would otherwise be free to acquire those locks in opposite order and deadlock. Ordering
-- every caller's lock acquisition the same way (topic name, then partition) rules that out.
order by t.name, tp.partition;

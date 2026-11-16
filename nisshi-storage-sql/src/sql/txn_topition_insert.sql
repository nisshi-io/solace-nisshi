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

insert into txn_topition
(txn_detail, topition)

select txn_d.id, tp.id

from

cluster c
join producer p on p.cluster = c.id
join topic t on t.cluster  = c.id
join topition tp on tp.topic = t.id
join txn on txn.cluster = c.id and txn.producer = p.id
join producer_epoch pe on pe.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id

where

c.name = $1
and t.name = $2
and tp.partition = $3
and txn.name = $4
and p.id = $5
and pe.epoch = $6

-- AddPartitionsToTxn is idempotent in Kafka: a partition already in this
-- transaction is a no-op, not an error. A client re-sending the same
-- partition (a retry, or a produce racing the first add) must not fail with
-- a unique violation on (txn_detail, topition).
on conflict (txn_detail, topition) do nothing;

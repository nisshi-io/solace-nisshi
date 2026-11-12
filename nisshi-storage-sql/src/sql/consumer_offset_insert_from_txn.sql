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

insert into consumer_offset
(consumer_group,
topition,
committed_offset,
leader_epoch,
timestamp,
metadata)

select

cg.id,
tp.id,
txn_oc_tp.committed_offset,
txn_oc_tp.leader_epoch,
txn_oc_tp.created_at,
txn_oc_tp.metadata

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join consumer_group cg on cg.cluster = c.id
join topic t on t.cluster = c.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join topition tp on tp.topic = t.id
join txn_offset_commit txn_oc on txn_oc.txn_detail = txn_d.id and txn_oc.consumer_group = cg.id
join txn_offset_commit_tp txn_oc_tp on txn_oc_tp.offset_commit = txn_oc.id and txn_oc_tp.topition = tp.id

where

c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4

on conflict (consumer_group, topition)

do update set

committed_offset = excluded.committed_offset,
leader_epoch = excluded.leader_epoch,
timestamp = excluded.timestamp,
last_updated = excluded.last_updated,
metadata = excluded.metadata;

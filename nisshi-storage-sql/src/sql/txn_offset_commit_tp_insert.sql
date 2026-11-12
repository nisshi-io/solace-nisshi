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

-- prepare txn_offset_commit_tp (text, text, text, integer, integer, text, integer) as
insert into txn_offset_commit_tp
(offset_commit, topition, committed_offset, leader_epoch, metadata)

select

oc.id,
tp.id,
$8,
$9,
$10

from

cluster c
join consumer_group cg on cg.cluster = c.id
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join txn_offset_commit oc on oc.txn_detail = txn_d.id and oc.consumer_group = cg.id

where

c.name = $1
and txn.name = $2
and cg.name = $3
and p.id = $4
and pe.epoch = $5
and t.name = $6
and tp.partition = $7;

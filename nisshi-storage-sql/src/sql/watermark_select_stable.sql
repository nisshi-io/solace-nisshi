-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright (c) Solace Corporation
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

-- Last Stable Offset Tracking: To do this, the broker must maintain in
-- memory the set of active transactions along with their initial
-- offsets. The last stable offset is always equal to the minimum
-- of the initial offsets across all active transactions.
--
-- prepare watermark (text, text, integer) as
select

min(txn_po.offset_start) as stable

from cluster c

join topic t on t.cluster = c.id
join txn on txn.cluster = c.id
join txn_topition txn_tp on txn_tp.txn = txn.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
join topition tp on tp.topic = t.id and txn_tp.topition = tp.id
join watermark w on w.topition = tp.id

where

c.name = $1
and t.name = $2
and tp.partition = $3;

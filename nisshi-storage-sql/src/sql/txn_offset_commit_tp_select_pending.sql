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

-- prepare txn_offset_commit_tp_select_pending (text, text) as

select distinct t.name as topic, tp.partition

from

cluster c
join consumer_group cg on cg.cluster = c.id
join txn on txn.cluster = c.id
join txn_detail txn_d on txn_d."transaction" = txn.id
join producer_epoch pe on pe.id = txn_d.producer_epoch and pe.producer = txn.producer
join txn_offset_commit txn_oc on txn_oc.txn_detail = txn_d.id and txn_oc.consumer_group = cg.id
join txn_offset_commit_tp txn_oc_tp on txn_oc_tp.offset_commit = txn_oc.id
join topition tp on tp.id = txn_oc_tp.topition
join topic t on t.id = tp.topic

where

c.name = $1
and cg.name = $2

-- the current epoch of the transaction: an InitProducerId that moves the
-- producer to a new epoch leaves the rows of an earlier epoch in place. An
-- end that waits behind an older open transaction keeps its rows until it
-- completes, at whichever epoch it was prepared.
and (
    txn_d.status in ('PREPARE_COMMIT', 'PREPARE_ABORT')
    or pe.epoch = (
        select max(current_pe.epoch)

        from producer_epoch current_pe
        join txn_detail current_txn_d
        on current_txn_d.producer_epoch = current_pe.id
        and current_txn_d."transaction" = txn.id

        where current_pe.producer = txn.producer
    )
);

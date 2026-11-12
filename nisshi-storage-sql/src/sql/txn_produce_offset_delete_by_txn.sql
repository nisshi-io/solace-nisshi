-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ Solace Corporation
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

delete from txn_produce_offset
where txn_produce_offset.txn_topition in (
    select txn_tp.id

    from cluster c
    join producer p on p.cluster = c.id
    join producer_epoch pe on pe.producer = p.id
    join txn on txn.cluster = c.id and txn.producer = p.id
    -- producer_epoch = pe.id is required here: without it, this deletes offset tracking
    -- for EVERY epoch of this (transaction_id, producer_id), not just the epoch in $4 --
    -- txn_detail is one row per (transaction, producer_epoch), reused across a producer's
    -- sequential transactions after each reconnect.
    join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
    join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id

    where

    c.name = $1
    and txn.name = $2
    and p.id = $3
    and pe.epoch = $4
);

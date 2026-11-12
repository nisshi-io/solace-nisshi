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

update txn_detail

set

started_at = current_timestamp,
status = 'BEGIN'

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id

where

c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4
and txn_detail."transaction" = txn.id
and txn_detail.producer_epoch = pe.id
-- Reset only a fresh row or one whose previous transaction has resolved --
-- never one still in flight (BEGIN/PREPARE_*). No started_at check needed:
-- it is only ever set together with status, so status alone decides.
and (
    txn_detail.status is null
    or txn_detail.status in ('COMMITTED', 'ABORTED')
);

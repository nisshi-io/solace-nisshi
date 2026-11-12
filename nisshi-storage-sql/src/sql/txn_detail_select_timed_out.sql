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

-- prepare txn_detail_select_timed_out (text, timestamptz) as
-- Returns transactions still in BEGIN past their timeout, for the abort sweep.

select txn.name, p.id as producer_id, pe.epoch as producer_epoch

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id

where

c.name = $1
and txn_d.status = 'BEGIN'
and txn_d.transaction_timeout_ms > 0 -- 0 means "no timeout", never expire it
and txn_d.started_at is not null
-- $2 is the sweep's "now"; both sides are absolute instants (timestamptz), so
-- the subtraction gives true elapsed time regardless of session timezone.
and (extract(epoch from cast($2 as timestamptz)) - extract(epoch from txn_d.started_at)) * 1000
    > txn_d.transaction_timeout_ms;

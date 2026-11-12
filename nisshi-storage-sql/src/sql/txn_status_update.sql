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

update txn_detail

set

status = $5,
last_updated = current_timestamp

from cluster c, producer p, producer_epoch pe, txn

where c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4
and p.cluster = c.id
and pe.producer = p.id
and txn.cluster = c.id
and txn.producer = p.id
and txn_detail.producer_epoch = pe.id
and txn_detail."transaction" = txn.id;

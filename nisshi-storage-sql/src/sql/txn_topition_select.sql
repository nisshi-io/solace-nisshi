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

select t.name, tp.partition, txnp.offset_start, txnp.offset_end
from cluster c,
producer p,
topic t,
topition tp,
txn,
txn_topition txn_tp
where c.name = $1
and p.id = $2
and p.epoch = $3
and t.name = $4
and tp.partition = $5
and txn.cluster = c.id
and txn.producer = p.id
and p.cluster = c.id
and t.cluster = c.id
and tp.topic = t.id
and txn_tp.txn = txn.id
and txn_tp.topition = tp.id;

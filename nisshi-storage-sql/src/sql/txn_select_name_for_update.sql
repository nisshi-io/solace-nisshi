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

-- prepare txn_select_name_for_update (text, text) as
-- Locks the transactional ID's txn row, so concurrent InitProducerId requests
-- for it check and bump the producer epoch one at a time.

select txn.id

from

cluster c
join txn on txn.cluster = c.id

where

c.name = $1
and txn.name = $2

for update of txn;

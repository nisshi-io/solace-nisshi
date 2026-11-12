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

update producer

set

epoch = epoch + 1,
sequence = 0

from

cluster c,
txn

where c.name = $1
and txn.name = $2
and txn.cluster = c.id
and txn.producer = producer.id
and producer.cluster = c.id

returning producer.id, producer.epoch;

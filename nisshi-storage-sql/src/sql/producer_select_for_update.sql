-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

select

coalesce(pd.sequence, 0) as sequence

from

cluster c
join producer p on p.cluster = c.id
join topic t on t.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join topition tp on tp.topic = t.id
left join producer_detail pd on pd.producer_epoch = pe.id and pd.topition = tp.id

where

c.name = $1
and t.name = $2
and tp.partition = $3
and p.id = $4
and pe.epoch = $5;

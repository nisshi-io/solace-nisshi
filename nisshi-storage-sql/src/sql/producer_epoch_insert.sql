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

-- prepare producer_epoch_insert(text, integer) as

with curr as (

select

p.id,
coalesce(max(pe.epoch) + 1, 0) as epoch

from

cluster c
join producer p on p.cluster = c.id
left join producer_epoch pe on pe.producer = p.id

where

c.name = $1
and p.id = $2

group by p.id

-- Epoch 32767 (i16::MAX) is the largest that the column and the wire hold. A
-- producer at that epoch gets no row: it is fenced already.
having coalesce(max(pe.epoch), -1) < 32767

)

insert into producer_epoch (producer, epoch)
select id, epoch from curr
returning epoch;

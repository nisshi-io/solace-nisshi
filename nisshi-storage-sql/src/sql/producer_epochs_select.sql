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

-- prepare producer_epochs_select (text, bigint, text, integer) as

select

max(pe.epoch) as newest_epoch,
max(case when pd.id is not null then pe.epoch end) as partition_epoch,

case
when exists (select 1 from txn x where x.producer = p.id) then 1
else 0
end as transactional

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
left join producer_detail pd
on pd.producer_epoch = pe.id
and pd.topition = (
    select tp.id
    from topic t
    join topition tp on tp.topic = t.id
    where t.cluster = c.id and t.name = $3 and tp.partition = $4
)

where

c.name = $1
and p.id = $2

group by p.id;

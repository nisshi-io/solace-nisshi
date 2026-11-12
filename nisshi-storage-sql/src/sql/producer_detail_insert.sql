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

insert into producer_detail (producer_epoch, topition, sequence)

select pe.id, tp.id, $6

from

cluster c
join producer p on p.cluster = c.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join producer_epoch pe on pe.producer = p.id

where

c.name = $1
and t.name = $2
and tp.partition = $3
and p.id = $4
and pe.epoch = $5

on conflict (producer_epoch, topition)

do update set

-- The next sequence wraps to 0 after 2147483647, as [`DefaultRecordBatch.incrementSequence`]
-- does, and $6 is the number of records that the batch adds. The condition compares against a
-- difference and not a sum, because the sum overflows a PostgreSQL integer.
--
-- [`DefaultRecordBatch.incrementSequence`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L556-L560
sequence = case
when producer_detail.sequence > 2147483647 - $6
then $6 - (2147483647 - producer_detail.sequence) - 1
else producer_detail.sequence + $6
end,
last_updated = excluded.last_updated

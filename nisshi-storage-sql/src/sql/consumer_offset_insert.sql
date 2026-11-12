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

insert into consumer_offset
(consumer_group, topition, committed_offset, leader_epoch, timestamp, metadata)

select cg.id, tp.id, $5, $6, $7, $8

from cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join consumer_group cg on cg.cluster = c.id

where c.name = $1
and t.name = $2
and tp.partition = $3
and cg.name = $4

on conflict (consumer_group, topition)
do update set
committed_offset = excluded.committed_offset,
leader_epoch = excluded.leader_epoch,
timestamp = excluded.timestamp,
metadata = excluded.metadata;

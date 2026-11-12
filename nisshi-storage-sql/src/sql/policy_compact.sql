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

-- The compacted partitions are found before record is read, and the CTE is materialized so that
-- the planner cannot move the join to record above the join to topic_configuration. When it
-- did, it read every partition of every topic, then discarded the rows of partitions that are
-- not compacted: on 48 million records, one sweep took 48 seconds and removed nothing.
with

compacted as materialized (
    select tp.id as topition
    from topition tp
    join topic t on t.id = tp.topic
    join cluster c on c.id = t.cluster
    join topic_configuration tc on tc.topic = t.id
    where c.name = $1
    and tc.name = 'cleanup.policy'
    and tc.value like '%compact%'
),

dup as (
    select
    r.topition as topition, r.k as k, max(r.offset_id) as offset_id
    from compacted
    join record r on r.topition = compacted.topition
    where r.k is not null
    group by r.topition, r.k having count(r.k) > 1
),

compaction as (
    select
    r.topition as topition, r.offset_id as offset_id
    from record r
    join dup on r.topition = dup.topition and r.k = dup.k
    where dup.offset_id > r.offset_id
)

delete from record
where (record.topition, record.offset_id) in (select * from compaction);

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

-- Kafka's retention.bytes is a per-partition limit: delete a partition's oldest records until the
-- bytes of its keys and values are within it, and always keep the newest record.
with

-- materialized makes SQLite choose the partitions that retention.bytes applies to before it reads
-- record. Without it, SQLite merges this query into sized and joins the LEFT JOIN after the tables
-- on its left, so it reads every record of a topic before it skips the topic for its
-- cleanup.policy, and holds the write lock while it does.
capped as materialized (
    select tp.id as topition, cast(tc.value as bigint) as cap
    from topition tp
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    join topic_configuration tc on tc.topic = t.id
    left join topic_configuration pol on pol.topic = t.id and pol.name = 'cleanup.policy'
    where c.name = $1
    and tc.name = 'retention.bytes'
    and tc.value glob '[0-9]*' and tc.value not glob '*[^0-9]*'
    -- A topic without a cleanup.policy has Kafka's default policy, delete.
    and coalesce(pol.value, 'delete') like '%delete%'
),

sized as (
    select r.topition, r.offset_id, capped.cap,
    sum(coalesce(length(r.k), 0) + coalesce(length(r.v), 0))
        over (partition by r.topition order by r.offset_id desc) as retained,
    row_number() over (partition by r.topition order by r.offset_id desc) as newest
    from record r
    join capped on capped.topition = r.topition
),

excess as (
    select topition, offset_id
    from sized
    where retained > cap
    and newest > 1
)

delete from record
where (record.topition, record.offset_id) in (select topition, offset_id from excess);

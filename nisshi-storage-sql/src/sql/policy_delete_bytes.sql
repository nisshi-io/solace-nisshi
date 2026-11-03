-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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
--
-- The bytes kept from an offset up rise as the offset falls, so the records to delete are the ones
-- at and below the first offset, counting down from the newest record, where they pass the limit.
-- Each partition is read from its newest record in primary key order, and the read stops at that
-- offset, so a sweep reads about as many records as a partition may keep and not the whole
-- partition. The delete is then a range of the primary key.
--
-- A lateral subquery per partition makes the planner start from the partitions that have a limit.
-- Joined, it read all of record before it tested the limit.
with

capped as (
    select tp.id as topition, cast(tc.value as bigint) as cap
    from topition tp
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    join topic_configuration tc on tc.topic = t.id
    left join topic_configuration pol on pol.topic = t.id and pol.name = 'cleanup.policy'
    where c.name = $1
    and tc.name = 'retention.bytes'
    -- A negative retention.bytes is no limit. The pattern allows at most 18 digits, so the cast to
    -- bigint never overflows and fails this statement for every topic. The broker rejects a value
    -- that isn't a long, but a topic created before the broker checked the value can hold one, and
    -- that topic keeps every record.
    and tc.value ~ '^[0-9]{1,18}$'
    -- A topic without a cleanup.policy has Kafka's default policy, delete.
    and coalesce(pol.value, 'delete') like '%delete%'
),

-- the highest offset to delete in each partition, absent when the partition is within its limit.
-- MATERIALIZED makes the planner compute it once, before the delete: merged into the delete's join,
-- it ran the read once for every record of the partition.
boundary as materialized (
    select capped.topition, over_limit.offset_id
    from capped
    cross join lateral (
        select sized.offset_id
        from (
            select r.offset_id,
            sum(coalesce(octet_length(r.k), 0) + coalesce(octet_length(r.v), 0))
                over newest_first as retained,
            row_number() over newest_first as newest
            from record r
            where r.topition = capped.topition
            window newest_first as (order by r.offset_id desc)
            order by r.offset_id desc
        ) sized
        where sized.retained > capped.cap
        and sized.newest > 1
        order by sized.offset_id desc
        limit 1
    ) over_limit
)

delete from record
using boundary
where record.topition = boundary.topition
and record.offset_id <= boundary.offset_id;

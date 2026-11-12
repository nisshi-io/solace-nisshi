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

with

-- materialized makes SQLite choose the partitions that retention applies to before it reads
-- record. Without it, SQLite merges this query into ancient and joins each LEFT JOIN after the
-- tables on its left, so it reads every record of a topic before it skips the topic for its
-- cleanup.policy or retention.ms, and holds the write lock while it does.
deletion as materialized (
    select
    tp.id as topition,
    coalesce(cast(trim(rt.value) as integer), $3) as retention_ms
    from topition tp
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    left join topic_configuration cp on cp.topic = t.id and cp.name = 'cleanup.policy'
    left join topic_configuration rt on rt.topic = t.id and rt.name = 'retention.ms'
    where c.name = $1
    -- A topic without a cleanup.policy has Kafka's default policy, delete.
    and coalesce(cp.value, 'delete') like '%delete%'
    -- A negative retention.ms keeps every record, because Kafka reads any negative value as no
    -- limit. The broker rejects a value below -1, and stores any other as a plain number, but a
    -- topic created before the broker checked the value can hold any text. A value that isn't a
    -- number, with an optional sign as Kafka allows, keeps every record, instead of being cast to 0
    -- and deleting every record.
    and case
        when rt.value is null then 1
        when (trim(rt.value) glob '[0-9]*' or trim(rt.value) glob '[-+][0-9]*')
            and substr(trim(rt.value), 2) not glob '*[^0-9]*'
            then cast(trim(rt.value) as integer) >= 0
        else 0
    end
),

ancient as (
    select r.topition, r.offset_id
    from record r
    join deletion del on del.topition = r.topition
    -- A record without a timestamp stores NO_TIMESTAMP, -1. Kafka expires such a record by when
    -- its log segment was last written ([LogSegment.largestTimestamp]), so this query expires it
    -- by created_at, the time that the broker appended it, in UTC seconds. Kafka applies the same
    -- rule to any negative timestamp, so this query does too.
    --
    -- created_at follows k and v in the row, so reading it for a record with a negative timestamp
    -- reads the whole row, including the overflow pages of a large value. A record with any other
    -- timestamp does not read created_at.
    --
    -- [LogSegment.largestTimestamp]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L855-L860
    where $2 - case
        when r.timestamp < 0 then cast(strftime('%s', r.created_at) as integer) * 1000
        else r.timestamp
    end > del.retention_ms
)

delete from record
where (record.topition, record.offset_id) in (select * from ancient);

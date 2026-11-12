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

-- Deletes the records that are older than their topic's retention. record has no index on
-- timestamp, so a query that tests every record for expiry reads the whole table: on 48 million
-- records, one sweep took 81 seconds when nothing had expired.
--
-- The time that a record was appended (created_at) rises with its offset in a partition, so the
-- records that have expired by that time are the ones below an offset that a binary search over
-- the primary key finds, with about 20 index probes in place of a scan. A probe looks at the first
-- record at or after the middle of the search range, and skips offsets that are no longer stored.
-- The search uses the append time and not the record's own timestamp, because the timestamp is
-- chosen by the producer: one record with a timestamp far in the future, from a skewed clock, would
-- otherwise stop the search and hold back the deletion of every record after it.
--
-- The delete still tests the timestamp of each record below that offset, so it never removes a
-- record that has not expired, and a record with a timestamp ahead of its neighbours only keeps
-- itself. A record with an old timestamp that was appended recently is deleted once its append time
-- has expired, which can be later than its timestamp alone would allow. created_at defaults to the
-- start time of the appending transaction, so appends that overlap in time can give a record a
-- smaller created_at than the record before it. The search can then stop just below an expired
-- record, and a later sweep deletes that record. A record just below the boundary is deleted at
-- once when its timestamp has expired, even if its own append time has not.
--
-- created_at is a timestamp without a time zone, in the time zone of the session that appended
-- the record, so it is converted with the TimeZone setting before it is compared with now.
with recursive

deletion as (
    select
    tp.id as topition,
    coalesce(cast(rt.value as bigint) / 1000, $3) as retention_secs,
    coalesce(w.low, 0) as low,
    coalesce(w.high, 0) as high
    from topition tp
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    left join topic_configuration cp on cp.topic = t.id and cp.name = 'cleanup.policy'
    left join topic_configuration rt on rt.topic = t.id and rt.name = 'retention.ms'
    join watermark w on w.topition = tp.id
    where c.name = $1
    -- A topic without a cleanup.policy has Kafka's default policy, delete.
    and coalesce(cp.value, 'delete') like '%delete%'
    and case
        when rt.value is null then true
        -- A negative retention.ms keeps every record, because Kafka reads any negative value as no
        -- limit. The broker rejects a value below -1, but a topic created before the broker checked
        -- the value can hold one. The pattern allows a sign, as Kafka does, and at most 18 digits,
        -- so the cast to bigint never overflows and fails this statement for every topic.
        when trim(rt.value) ~ '^[-+]?[0-9]{1,18}$' then cast(rt.value as bigint) >= 0
        -- A retention.ms stored before the broker validated it can be any text. The topic keeps
        -- every record, because casting that text fails this statement for every topic.
        else false
    end
),

-- Each step narrows lo..hi, where lo is the first offset that may be kept and hi the first offset
-- that is known to be kept. An expired probe moves lo past it. A probe that is not expired moves hi
-- down to it. When no record is stored from the middle up to hi, which is the case when
-- retention or compaction has already removed those offsets, hi moves down to the middle: each
-- branch shrinks the range, so the search ends.
search as (
    select topition, retention_secs, low as lo, high as hi
    from deletion

    union all

    select
    s.topition,
    s.retention_secs,
    case when probe.expired then probe.offset_id + 1 else s.lo end,
    case when probe.expired then s.hi else coalesce(probe.offset_id, (s.lo + s.hi) / 2) end
    from search s
    left join lateral (
        select
        r.offset_id,
        (extract(epoch from cast($2 as timestamp))
            - extract(epoch from r.created_at at time zone current_setting('TimeZone'))) > s.retention_secs as expired
        from record r
        where r.topition = s.topition
        and r.offset_id >= (s.lo + s.hi) / 2
        and r.offset_id < s.hi
        order by r.offset_id
        limit 1) probe on true
    where s.lo < s.hi
),

boundary as (
    select topition, retention_secs, lo as offset_id
    from search
    where lo >= hi
)

-- offset 0 stops the planner from merging the lateral subquery into the join above it. Merged, the
-- join read all of record and hash joined it to boundary; unmerged, it reads the records below the
-- boundary offset of each partition with one scan of the primary key.
delete from record
where (record.topition, record.offset_id) in (
    select r.topition, r.offset_id
    from boundary b
    cross join lateral (
        select rr.topition, rr.offset_id
        from record rr
        where rr.topition = b.topition
        and rr.offset_id < b.offset_id
        and (extract(epoch from cast($2 as timestamp)) - extract(epoch from rr.timestamp)) > b.retention_secs
        offset 0) r);

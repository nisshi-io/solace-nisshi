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

-- Fills one topic with generated records, so queries can be checked against large volumes
-- without producing through the broker. Run it with `run.sh seed`, or directly:
--
--   psql -v rows=1000000 -v partitions=16 -f seed.sql
--
-- Variables (psql -v name=value), all optional:
--
--   cluster      cluster name, must match the broker's CLUSTER_ID  (nisshi_cluster)
--   topic        topic to create, an existing topic of this name is replaced  (seed)
--   partitions   number of partitions  (16)
--   rows         records per partition  (1000000)
--   value_size   bytes in each record value  (1024)
--   key_pct      percent of records that have a key  (10)
--   header_pct   percent of records that have two headers  (10)
--
-- Disk used is roughly partitions * rows * (value_size + ~100) bytes, so 16 * 1,000,000 records
-- of 1 KiB is about 18 GB. Check that the Docker VM has that much free before starting.
--
-- Row count drives which plan the planner picks, and value_size drives temp file spills and I/O,
-- so a small value_size scales the row count further for the same disk when only plans matter.
--
-- Values are random, so they do not compress, as with real payloads. They are drawn from a pool
-- of 256 distinct values rather than generated per row: generating a random value per row costs
-- more than the insert itself.

\if :{?cluster} \else \set cluster nisshi_cluster \endif
\if :{?topic} \else \set topic seed \endif
\if :{?partitions} \else \set partitions 16 \endif
\if :{?rows} \else \set rows 1000000 \endif
\if :{?value_size} \else \set value_size 1024 \endif
\if :{?key_pct} \else \set key_pct 10 \endif
\if :{?header_pct} \else \set header_pct 10 \endif

\set ON_ERROR_STOP on

-- the load is rebuilt from scratch if it fails, so there is nothing to wait for at commit
set synchronous_commit = off;

begin;

create function pg_temp.random_bytes(n int) returns bytea language sql volatile as $$
    select substr(string_agg(decode(replace(gen_random_uuid()::text, '-', ''), 'hex'), ''::bytea), 1, n)
    from generate_series(1, (n + 15) / 16)
$$;

create temp table pool on commit drop as
select i, pg_temp.random_bytes(:value_size) as v from generate_series(0, 255) i;

insert into cluster (name) values (:'cluster') on conflict (name) do nothing;

-- replace any earlier seed of this topic: topition, watermark, record and header cascade from it
delete from topic
where name = :'topic' and cluster = (select id from cluster where name = :'cluster');

insert into topic (cluster, name, partitions, replication_factor)
select id, :'topic', :partitions, 1 from cluster where name = :'cluster';

insert into topition (topic, partition)
select t.id, p
from topic t join cluster c on c.id = t.cluster, generate_series(0, :partitions - 1) p
where c.name = :'cluster' and t.name = :'topic';

-- high is the next offset to be written, as the produce path leaves it
insert into watermark (topition, low, high)
select tp.id, 0, :rows
from topition tp join topic t on t.id = tp.topic join cluster c on c.id = t.cluster
where c.name = :'cluster' and t.name = :'topic';

-- one statement per partition, so progress shows and no statement outgrows the working memory
select format($q$
    insert into record (topition, offset_id, attributes, producer_id, producer_epoch, timestamp, k, v)
    select %1$s, o, 0, -1, -1,
        now() - (%2$s - o) * interval '1 millisecond',
        case when o %% 100 < %3$s then convert_to('key-' || (o %% 1000), 'UTF-8') end,
        (select v from pool where i = (o::bigint * 7919 + %1$s) %% 256)
    from generate_series(0, %2$s - 1) o
$q$, tp.id, :rows, :key_pct) as stmt
from topition tp join topic t on t.id = tp.topic join cluster c on c.id = t.cluster
where c.name = :'cluster' and t.name = :'topic'
order by tp.partition
\gexec

select format($q$
    insert into header (topition, offset_id, ordinal, k, v)
    select %1$s, o, n, convert_to('header-' || n, 'UTF-8'), convert_to('value-' || o, 'UTF-8')
    from generate_series(0, %2$s - 1) o, generate_series(0, 1) n
    where o %% 100 < %3$s
$q$, tp.id, :rows, :header_pct) as stmt
from topition tp join topic t on t.id = tp.topic join cluster c on c.id = t.cluster
where c.name = :'cluster' and t.name = :'topic'
order by tp.partition
\gexec

commit;

-- the planner chooses between a bitmap scan and an ordered walk of the primary key from its
-- statistics, so plans on unanalyzed tables say nothing about the real ones
analyze record;
analyze header;
analyze topition;
analyze watermark;

select
    (select count(*) from record r join topition tp on tp.id = r.topition
        join topic t on t.id = tp.topic where t.name = :'topic') as records,
    (select count(*) from header h join topition tp on tp.id = h.topition
        join topic t on t.id = tp.topic where t.name = :'topic') as headers,
    pg_size_pretty(pg_total_relation_size('record_default')) as record_size,
    pg_size_pretty(pg_total_relation_size('header_default')) as header_size;

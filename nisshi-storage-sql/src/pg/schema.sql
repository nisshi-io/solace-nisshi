-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ Solace Corporation
--
-- Contributors outside Solace:
--   Or Hayat
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

create table if not exists cluster (
    id int generated always as identity primary key,
    name text not null,
    unique (name),
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create table if not exists topic (
    id int generated always as identity primary key,
    cluster int references cluster (id) on delete cascade not null,
    name text not null,
    unique (cluster, name),
    uuid uuid default gen_random_uuid (),
    partitions int not null,
    replication_factor int not null,
    is_internal bool default false not null,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create table if not exists virtual_topic (
    id int generated always as identity primary key,
    topic int references topic (id) on delete cascade not null,
    k bytea,
    uuid uuid not null unique,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null,
    unique (topic, k)
);

create
or replace view v_topic as
select
    c.name as cluster,
    t.name as topic,
    t.uuid as uuid,
    t.partitions as partitions,
    t.replication_factor as replication_factor,
    t.is_internal as is_internal
from
    cluster c
    join topic t on t.cluster = c.id;

create table if not exists topition (
    id int generated always as identity primary key,
    topic int references topic (id) on delete cascade,
    partition int,
    unique (topic, partition),
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_topition as
select
    c.name as cluster,
    t.name as topic,
    tp.partition as partition
from
    cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id;

create table if not exists watermark (
    id int generated always as identity primary key,
    topition int references topition (id) on delete cascade,
    unique (topition),
    low bigint,
    high bigint,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create table if not exists topic_configuration (
    id int generated always as identity primary key,
    topic int references topic (id) on delete cascade,
    name text not null,
    unique (topic, name),
    value text,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_topic_configuration as
select
    c.name as cluster,
    t.name as topic,
    tc.name as name,
    tc.value as value,
    tc.created_at as created_at,
    tc.last_updated as last_updated
from
    cluster c
    join topic t on t.cluster = c.id
    join topic_configuration tc on tc.topic = t.id;

create table if not exists record (
    topition int references topition (id) on delete cascade,
    offset_id bigint not null,
    primary key (topition, offset_id),
    transaction_id xid8 default pg_current_xact_id() not null,
    attributes smallint,
    producer_id bigint,
    producer_epoch smallint,
    timestamp timestamp,
    k bytea,
    v bytea,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
)
partition by
    list (topition);

create table if not exists record_default partition of record default;

create
or replace view v_record as
select
    c.name as cluster,
    t.name as topic,
    tp.partition as partition,
    r.offset_id,
    r.producer_id,
    r.producer_epoch,
    r.timestamp
from
    cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join record r on r.topition = tp.id
order by
    c.name,
    t.name,
    tp.partition,
    r.offset_id;

create table if not exists header (
    topition int,
    offset_id bigint,
    ordinal int,
    primary key (topition, offset_id, ordinal),
    foreign key (topition, offset_id) references record (topition, offset_id) on delete cascade,
    k bytea,
    v bytea,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
)
partition by
    list (topition);

create table if not exists header_default partition of header default;

create table if not exists consumer_group (
    id int generated always as identity primary key,
    cluster int references cluster (id) on delete cascade not null,
    name text not null,
    unique (cluster, name),
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_consumer_group as
select
    c.name as cluster,
    cg.name as consumer_group
from
    cluster c
    join consumer_group cg on cg.cluster = c.id;

create table if not exists consumer_group_detail (
    id int generated always as identity primary key,
    consumer_group int references consumer_group (id) on delete cascade,
    unique (consumer_group),
    e_tag uuid not null,
    detail json not null,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_consumer_group_detail as
select
    c.name as cluster,
    cg.name as consumer_group,
    cgd.e_tag as e_tag,
    json_object_field (cgd.detail, 'generation_id') as generation_id,
    json_object_field (cgd.detail, 'rebalance_timeout_ms') as rebalance_timeout_ms,
    json_object_field (cgd.detail, 'session_timeout_ms') as session_timeout_ms,
    json_object_field (cgd.detail, 'skip_assignment') as skip_assignment,
    array(
        select
            json_object_keys (json_object_field (cgd.detail, 'members'))
    ) as members
from
    cluster c
    join consumer_group cg on cg.cluster = c.id
    join consumer_group_detail cgd on cgd.consumer_group = cg.id;

create table if not exists consumer_offset (
    id int generated always as identity primary key,
    consumer_group int references consumer_group (id) on delete cascade,
    topition int references topition (id) on delete cascade,
    unique (consumer_group, topition),
    committed_offset bigint,
    leader_epoch int,
    timestamp timestamp,
    metadata text,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_consumer_offset as
select
    c.name as cluster,
    cg.name as consumer_group,
    t.name as topic,
    tp.partition as partition,
    co.committed_offset as committed_offset,
    co.leader_epoch as leader_epoch,
    co.timestamp as timestamp,
    co.metadata as metadata
from
    cluster c
    join consumer_group cg on cg.cluster = c.id
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join consumer_offset co on co.consumer_group = cg.id
    and co.topition = tp.id;

-- non transactional idempotent producer
--
create table if not exists producer (
    id bigint generated by default as identity primary key,
    cluster int references cluster (id) on delete cascade not null,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create table if not exists producer_epoch (
    id int generated by default as identity primary key,
    producer bigint references producer (id) on delete cascade,
    epoch smallint default 0 not null,
    unique (producer, epoch),
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_producer_epoch as
select
    c.name as cluster,
    p.id as producer_id,
    pe.epoch as producer_epoch
from
    cluster c
    join producer p on p.cluster = c.id
    join producer_epoch pe on pe.producer = p.id;

create table if not exists producer_detail (
    id bigint generated by default as identity primary key,
    producer_epoch int references producer_epoch (id) on delete cascade,
    topition int references topition (id) on delete cascade,
    unique (producer_epoch, topition),
    sequence int default 0 not null,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_producer_detail as
select
    c.name as cluster,
    p.id as producer_id,
    pe.epoch as producer_epoch,
    t.name as topic,
    tp.partition as partition,
    pd.sequence as sequence
from
    cluster c
    join producer p on p.cluster = c.id
    join producer_epoch pe on pe.producer = p.id
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join producer_detail pd on pd.producer_epoch = pe.id
    and pd.topition = tp.id;

-- transactional, including idempotent producer
--
create table if not exists txn (
    id bigint generated always as identity primary key,
    cluster int references cluster (id) on delete cascade,
    name text,
    unique (cluster, name),
    producer bigint references producer (id) on delete cascade,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_txn as
select
    c.name as cluster,
    txn.name as txn
from
    cluster c
    join txn on txn.cluster = c.id;

create table if not exists txn_detail (
    id bigint generated always as identity primary key,
    transaction bigint references txn (id) on delete cascade,
    producer_epoch int references producer_epoch (id) on delete cascade,
    unique (transaction, producer_epoch),
    transaction_timeout_ms int not null,
    -- AddPartitionsToTxnRequest:
    -- If this is the first partition added to the transaction,
    -- the coordinator will also start the transaction timer
    --
    -- timestamptz: the timeout sweep subtracts this from "now" to get elapsed
    -- time; a zoneless timestamp would be off by the server's UTC offset.
    started_at timestamptz,
    -- BEGIN, PREPARE_COMMIT, PREPARE_ABORT, COMMITTED or ABORTED
    --
    status text,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_txn_detail as
select
    txn_d.id as id,
    c.name as cluster,
    txn.name as txn,
    p.id as producer_id,
    pe.epoch as producer_epoch,
    t.name as topic,
    tp.partition as partition,
    pd.sequence as sequence,
    txn_d.transaction_timeout_ms as transaction_timeout_ms,
    txn_d.started_at as started_at,
    txn_d.status as status
from
    cluster c
    join txn on txn.cluster = c.id
    join producer p on p.cluster = c.id
    join producer_epoch pe on pe.producer = p.id
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join producer_detail pd on pd.producer_epoch = pe.id
    and pd.topition = tp.id
    join txn_detail txn_d on txn_d.transaction = txn.id
    and txn_d.producer_epoch = pe.id;

-- AddPartitionsToTxnRequest
--
create table if not exists txn_topition (
    id int generated always as identity primary key,
    txn_detail int references txn_detail (id) on delete cascade,
    topition int references topition (id) on delete cascade,
    unique (txn_detail, topition),
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_txn_topition as
select
    c.name as cluster,
    txn.name as txn,
    p.id as producer_id,
    pe.epoch as producer_epoch,
    t.name as topic,
    tp.partition as partition
from
    cluster c
    join producer p on p.cluster = c.id
    join producer_epoch pe on pe.producer = p.id
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join txn on txn.cluster = c.id
    and txn.producer = p.id
    join txn_detail txn_d on txn_d.transaction = txn.id
    and txn_d.producer_epoch = pe.id
    join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id
    and txn_tp.topition = tp.id;

create table if not exists txn_produce_offset (
    id int generated always as identity primary key,
    txn_topition int references txn_topition (id) on delete cascade,
    unique (txn_topition),
    offset_start bigint,
    offset_end bigint,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_txn_produce_offset as
select
    c.name as cluster,
    p.id producer_id,
    pe.epoch as producer_epoch,
    txn.name as txn,
    txn_d.status as status,
    t.name as topic,
    tp.partition as partition,
    txn_po.offset_start as offset_start,
    txn_po.offset_end as offset_end
from
    cluster c
    join topic t on t.cluster = c.id
    join producer p on p.cluster = c.id
    join producer_epoch pe on pe.producer = pe.id
    join txn on txn.cluster = c.id
    and txn.producer = p.id
    join txn_detail txn_d on txn_d.transaction = txn.id
    and txn_d.producer_epoch = pe.id
    join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id
    join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
    join topition tp on tp.topic = t.id
    and txn_tp.topition = tp.id
order by
    c.name,
    txn.name,
    t.name,
    tp.partition,
    p.id,
    pe.epoch;

create table if not exists txn_offset_commit (
    id int generated always as identity primary key,
    txn_detail int references txn_detail (id) on delete cascade,
    consumer_group int references consumer_group (id) on delete cascade,
    unique (txn_detail, consumer_group),
    generation_id int,
    member_id text,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create table if not exists txn_offset_commit_tp (
    id int generated always as identity primary key,
    offset_commit int references txn_offset_commit (id) on delete cascade,
    topition int references topition (id) on delete cascade,
    unique (offset_commit, topition),
    committed_offset bigint,
    leader_epoch int,
    metadata text,
    last_updated timestamp default current_timestamp not null,
    created_at timestamp default current_timestamp not null
);

create
or replace view v_txn_consumer_offset_tp as
select
    c.name as cluster,
    p.id as producer_id,
    pe.epoch as producer_epoch,
    txn.name as txn,
    t.name as topic,
    tp.partition as partition,
    txn_oc_tp.committed_offset as committed_offset,
    txn_oc_tp.leader_epoch,
    txn_oc_tp.created_at,
    txn_oc_tp.metadata
from
    cluster c
    join consumer_group cg on cg.cluster = c.id
    join producer p on p.cluster = c.id
    join producer_epoch pe on pe.producer = pe.id
    join txn on txn.cluster = c.id
    and txn.producer = p.id
    join txn_detail txn_d on txn_d.transaction = txn.id
    and txn_d.producer_epoch = pe.id
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join txn_offset_commit txn_oc on txn_oc.txn_detail = txn_d.id
    and txn_oc.consumer_group = cg.id
    join txn_offset_commit_tp txn_oc_tp on txn_oc_tp.offset_commit = txn_oc.id
    and txn_oc_tp.topition = tp.id
order by
    c.name,
    txn.name,
    t.name,
    tp.partition,
    p.id,
    pe.epoch;

create
or replace view v_watermark as
with
    stable as (
        select
            t.id as topic,
            tp.id as topition,
            min(txn_po.offset_start) as
        offset
        from
            cluster c
            join topic t on t.cluster = c.id
            join topition tp on tp.topic = t.id
            join txn on txn.cluster = c.id
            join txn_detail txn_d on txn_d.transaction = txn.id
            join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id
            and txn_tp.topition = tp.id
            join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
        where
            txn_d.status = 'PREPARE_COMMIT'
            or txn_d.status = 'PREPARE_ABORT'
            or txn_d.status = 'BEGIN'
        group by
            t.id,
            tp.id
    )
select
    c.name as cluster,
    t.name as topic,
    tp.partition as partition,
    coalesce(w.low, 0) as low,
    coalesce(s.offset, coalesce(w.high, 0)) as stable,
    coalesce(w.high, 0) as high
from
    cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    left join watermark w on w.topition = tp.id
    left join stable s on s.topic = t.id
    and s.topition = tp.id
order by
    c.name,
    t.name,
    tp.partition;

create table if not exists scram_credential (
  id bigint generated by default as identity primary key,
  cluster int references cluster (id) on delete cascade not null,
  username text,
  mechanism integer,
  salt bytea not null,
  iterations integer not null,
  stored_key bytea not null,
  server_key bytea not null,
  unique (cluster, username, mechanism),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

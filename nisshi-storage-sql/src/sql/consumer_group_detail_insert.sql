-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

-- prepare cg_update (text, text, uuid, uuid, json) as
insert into consumer_group_detail
(consumer_group, e_tag, detail)

select cg.id, $4, $5

from cluster c
join consumer_group cg on cg.cluster = c.id

where c.name = $1
and cg.name = $2

on conflict (consumer_group)

do update set

detail = excluded.detail,
last_updated = excluded.last_updated,
e_tag = $4

where consumer_group_detail.e_tag = $3

returning $2 as consumer_group_name, $1 as cluster_name, e_tag, detail;

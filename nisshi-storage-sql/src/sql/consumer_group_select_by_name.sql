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

select cg.name, cgd.detail

from

cluster c
join consumer_group cg on cg.cluster = c.id
left join consumer_group_detail cgd on cgd.consumer_group = cg.id

where c.name = $1
and cg.name = $2;

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

-- prepare topic_configuration_select_keys (text, text, text[]) as
--
-- $1 is the cluster's name, $2 the topic's name, and $3 the config keys to read.
--
-- Returns one row for each key in $3 that the topic sets. For a topic that sets none of these keys,
-- it returns one row with a null name. So the query returns zero rows only when the topic does not
-- exist.
select tc.name, tc.value

from

cluster c
join topic t on t.cluster = c.id
left join topic_configuration tc on tc.topic = t.id and tc.name = any($3)

where

c.name = $1
and t.name = $2;

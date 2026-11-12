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

-- prepare topic_select_name_for_update (text, text) as
-- Locks the topic row, so that an alter of a topic's configuration and a
-- delete of that topic apply one after the other. Both take this lock before
-- they write any other row of the topic, so they can't deadlock.

select t.id

from

cluster c
join topic t on t.cluster = c.id

where

c.name = $1
and t.name = $2

for no key update of t;

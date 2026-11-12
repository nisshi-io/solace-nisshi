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

-- lite/watermark_select_for_write.sql must select the same columns.

select

w.low, w.high, tc.value as max_message_bytes

from

cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join watermark w on w.topition = tp.id
left join topic_configuration tc
on tc.topic = t.id and tc.name = 'max.message.bytes'

where

c.name = $1
and t.name = $2
and tp.partition = $3

-- The query locks the watermark row, so that the writes to one partition assign
-- their offsets one after the other. The query locks only that row. It joins
-- every partition of a cluster to the same cluster row, and every partition of
-- a topic to the same topic row. If the query locked either of those rows, each
-- produce would wait for the produces to other partitions.
for no key update of w;

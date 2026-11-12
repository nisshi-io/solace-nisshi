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

-- prepare record_delete_below_log_start (text) as

-- Maintenance runs this to delete every record below its partition's log
-- start (watermark.low). DeleteRecords on PostgreSQL commits the new
-- watermark.low first, and then deletes the records below it in a second
-- transaction. When that second transaction fails, the records stay stored
-- until this query deletes them.

delete from record

using cluster c, topic t, topition tp, watermark w

where

c.name = $1
and t.cluster = c.id
and tp.topic = t.id
and w.topition = tp.id
and record.topition = tp.id
and w.low is not null
and record.offset_id < w.low;

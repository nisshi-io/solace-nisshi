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

-- Used by DeleteRecords: removes every record strictly below the new low
-- watermark (the resolved cutoff offset) for one topic partition. The
-- cutoff itself must already be validated (see
-- `nisshi_storage::delete_records_cutoff`) before this runs.
delete from record
where record.topition in (
    select tp.id
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    where c.name = $1
    and t.name = $2
    and tp.partition = $3
)
and record.offset_id < $4;

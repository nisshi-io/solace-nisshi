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

pivot (
  select name, regexp_extract(filename, 'results-(.+)\.csv$', 1) as storage, case when outcome = 'PASS' then 'Pass ✅' when outcome = 'SKIP' then 'Skipped ⏭️' else 'Fail ❌' end as outcome from
  read_csv('results/results-*.csv', header = false, names = ['name', 'outcome'], union_by_name = true, filename = true)
)
on storage
using first(outcome)
group by name order by name;

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

-- Turso rejects `not null` after a `references` clause, so this file differs from the one in
-- `ddl/` by the order of the column constraints.
create table if not exists scram_credential (
  id integer primary key autoincrement,
  cluster integer not null references cluster (id) on delete cascade,
  username text not null,
  mechanism integer not null,
  salt blob not null,
  iterations integer not null,
  stored_key blob not null,
  server_key blob not null,
  last_updated text default current_timestamp not null,
  created_at text default current_timestamp not null,
  unique(cluster, username, mechanism)
);

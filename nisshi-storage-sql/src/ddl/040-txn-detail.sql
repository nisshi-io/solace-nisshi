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

create table if not exists txn_detail (
    id integer primary key autoincrement,
    "transaction" integer references txn (id),
    producer_epoch integer references producer_epoch (id),
    transaction_timeout_ms integer not null,
    -- AddPartitionsToTxnRequest:
    -- If this is the first partition added to the transaction,
    -- the coordinator will also start the transaction timer
    started_at text,
    -- BEGIN, PREPARE_COMMIT, PREPARE_ABORT, COMMITTED or ABORTED
    --
    status text,
    last_updated datetime default current_timestamp not null,
    created_at datetime default current_timestamp not null,
    unique ("transaction", producer_epoch)
);

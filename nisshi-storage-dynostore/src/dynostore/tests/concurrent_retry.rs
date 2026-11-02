// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A produce checks an idempotent batch's sequence before it writes the record, and advances the
//! sequence after. A retry of the same batch can pass the check while the first attempt waits
//! on its record write. Whichever attempt advances second must find the other's batch and
//! advance nothing, or the producer's next batch would be out of order.

use std::assert_matches;

use bytes::Bytes;
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage as _, Topition};

use crate::dynostore::{
    DynoStore,
    tests::{
        held_put_store::{self, HeldPutStore},
        init_tracing,
    },
};

fn idempotent_batch(producer_id: i64, base_sequence: i32) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .producer_id(producer_id)
        .producer_epoch(0)
        .base_sequence(base_sequence)
        .record(Record::builder().value(Bytes::from_static(b"value").into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

#[tokio::test]
async fn retry_that_advances_first_leaves_the_sequence_advanced_once() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = HeldPutStore::new(held_put_store::is_batch);
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new("abc", 0);
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;
    let record_write = object_store.hold_next_put();

    let first_attempt = storage.produce(None, &topition, idempotent_batch(producer.id, 0)?);

    let retry = async {
        record_write.wait_until_held().await?;

        let retry = storage
            .produce(None, &topition, idempotent_batch(producer.id, 0)?)
            .await;

        record_write.release();
        retry
    };

    let (first_attempt, retry) = tokio::join!(first_attempt, retry);

    assert_eq!(1, retry?);
    assert_matches!(
        first_attempt,
        Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
    );

    assert_eq!(
        2,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 1)?)
            .await?
    );

    Ok(())
}

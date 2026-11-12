// Copyright ⓒ Solace Corporation
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

//! A store with a lake writes a batch to the lake only for a topic whose schema validation is on,
//! because the lake converts each record through the schema and fails on a record that breaks it.

use bytes::Bytes;
use nisshi_sans_io::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
    topic_config,
};
use nisshi_schema::{Registry, lake::House};
use nisshi_storage::{Error, Storage as _, Topition};
use object_store::memory::InMemory;
use url::Url;

use crate::dynostore::{DynoStore, tests::init_tracing};

const TOPIC: &str = "person";

/// A record whose value is not JSON, so that the lake cannot convert it with the person schema.
fn unconvertible_person() -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(b"\"345-67-6543\"")))
                .value(Some(Bytes::from_static(b"not json"))),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Returns a store with a Parquet lake that knows the person schema, and the topic `person` with
/// one partition and the given configs.
async fn person_storage(configs: Vec<CreatableTopicConfig>) -> Result<DynoStore, Error> {
    // The lake writes nothing in these tests, because the record in them fails to convert. Its
    // location is the build directory, so that a write that a test did not expect stays out of
    // the source tree.
    let registry = Url::parse("file://../etc/schema")
        .map_err(Error::from)
        .and_then(|url| {
            Registry::builder_try_from_url(&url)
                .map(|builder| builder.build())
                .map_err(Error::from)
        })?;

    let lake = House::parquet()
        .location(Url::parse("file://../target")?)
        .schema_registry(registry)
        .build()?;

    let storage = DynoStore::new("nisshi", 111, InMemory::new())?.lake(Some(lake));

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(storage)
}

/// Shows that the lake fails on the record, so that the test below passes because the store does
/// not write the record to the lake and not because the lake accepts it.
#[tokio::test]
async fn a_record_that_breaks_the_schema_fails_in_the_lake_when_validation_is_on()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = person_storage(vec![]).await?;

    assert!(
        storage
            .produce(None, &Topition::new(TOPIC, 0), unconvertible_person()?)
            .await
            .is_err()
    );

    Ok(())
}

#[tokio::test]
async fn a_topic_with_validation_off_is_not_written_to_the_lake() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = person_storage(vec![
        CreatableTopicConfig::default()
            .name(topic_config::SCHEMA_VALIDATION.into())
            .value(Some("false".into())),
    ])
    .await?;

    assert_eq!(
        0,
        storage
            .produce(None, &Topition::new(TOPIC, 0), unconvertible_person()?)
            .await?
    );

    Ok(())
}

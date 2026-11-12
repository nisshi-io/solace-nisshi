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

mod doctest_template {
    use std::sync::Arc;

    use crate::common::init_tracing;
    use nisshi_broker::Error;
    use nisshi_sans_io::{
        CreateTopicsRequest, ErrorCode, FetchRequest, RequestInput,
        create_topics_request::CreatableTopic,
        fetch_request::{FetchPartition, FetchTopic},
    };
    use nisshi_storage::{
        CreateTopicsService, CreateTopicsValidationLayer, FetchService, FetchValidationLayer,
        StorageContainer,
    };
    use rama::{Layer as _, Service, extensions::Extensions};
    use url::Url;

    #[tokio::test]
    async fn req() -> Result<(), Error> {
        let _guard = init_tracing()?;

        const CLUSTER_ID: &str = "nisshi";
        const NODE_ID: i32 = 111;
        const HOST: &str = "localhost";
        const PORT: i32 = 9092;

        let builder = {
            let mut builder = StorageContainer::builder();
            builder.with_factory(Arc::new(nisshi_storage_dynostore::MemoryEngineFactory));

            builder
        };

        let storage = builder
            .cluster_id(CLUSTER_ID)
            .node_id(NODE_ID)
            .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
            .storage(Url::parse("memory://nisshi/")?)
            .build()
            .await?;

        let extensions = Extensions::default();

        let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
            storage: storage.clone(),
        });

        let name = "abcba";

        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(5)
                            .replication_factor(3)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ]))
                    .validate_only(Some(false)),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let fetch = FetchValidationLayer::new().layer(FetchService {
            storage: storage.clone(),
        });

        let partition = 0;

        let response = fetch
            .serve(RequestInput {
                request: FetchRequest::default()
                    .topics(Some(
                        [FetchTopic::default()
                            .topic(Some(name.into()))
                            .partitions(Some(
                                [FetchPartition::default().partition(partition)].into(),
                            ))]
                        .into(),
                    ))
                    .max_bytes(Some(0))
                    .max_wait_ms(5_000),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        Ok(())
    }
}

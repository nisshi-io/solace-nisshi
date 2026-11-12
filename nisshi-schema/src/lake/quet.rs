// Copyright (c) Solace Corporation
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

use std::{env, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use nisshi_sans_io::{describe_configs_response::DescribeConfigsResult, record::inflated::Batch};
use object_store::{
    DynObjectStore, PutMode, PutOptions, PutPayload,
    aws::{AmazonS3Builder, S3ConditionalPut},
    local::LocalFileSystem,
    path::Path,
};
use parquet::arrow::AsyncArrowWriter;
use tracing::debug;
use url::Url;

use crate::{
    AsArrow as _, Error, Registry, Result,
    lake::{LakeHouse, LakeHouseType},
};

use super::House;

#[derive(Clone, Debug, Default)]
pub struct Builder<L = PhantomData<Url>, R = PhantomData<Registry>> {
    location: L,
    schema_registry: R,
}

impl<L> Builder<L> {
    pub fn location(self, location: Url) -> Builder<Url> {
        Builder {
            location,
            schema_registry: self.schema_registry,
        }
    }

    pub fn schema_registry(self, schema_registry: Registry) -> Builder<L, Registry> {
        Builder {
            location: self.location,
            schema_registry,
        }
    }
}

impl Builder<Url, Registry> {
    pub fn build(self) -> Result<House> {
        Parquet::try_from(self).map(House::Parquet)
    }
}

#[derive(Clone, Debug)]
pub struct Parquet {
    object_store: Arc<DynObjectStore>,
    schema_registry: Registry,
}

#[async_trait]
impl LakeHouse for Parquet {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        inflated: &Batch,
        _config: DescribeConfigsResult,
    ) -> Result<()> {
        let record_batch = self
            .schema_registry
            .as_arrow(topic, partition, inflated, LakeHouseType::Parquet)
            .await?;

        let payload = {
            let mut buffer = Vec::new();
            let mut writer = AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
            writer.write(&record_batch).await?;
            _ = writer.close().await?;
            PutPayload::from(Bytes::from(buffer))
        };

        let location = Path::from(format!("{topic}/{partition:0>10}/{offset:0>20}.parquet"));

        self.object_store
            .put_opts(
                &location,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .inspect(|put_result| debug!(?put_result))
            .and(Ok(()))
            .map_err(Into::into)
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Parquet)
    }
}

impl TryFrom<Builder<Url, Registry>> for Parquet {
    type Error = Error;

    fn try_from(value: Builder<Url, Registry>) -> Result<Self, Self::Error> {
        match value.location.scheme() {
            "s3" => {
                let bucket_name = value.location.host_str().unwrap_or("lake");

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()
                    .map(Arc::new)
                    .map(|object_store| Self {
                        object_store,
                        schema_registry: value.schema_registry,
                    })
                    .map_err(Into::into)
            }

            "file" => {
                let mut path = env::current_dir().inspect(|current_dir| debug!(?current_dir))?;

                if let Some(domain) = value.location.domain() {
                    path.push(domain);
                }

                if let Some(relative) = value.location.path().strip_prefix("/") {
                    path.push(relative);
                } else {
                    path.push(value.location.path());
                }

                debug!(?path);

                LocalFileSystem::new_with_prefix(path)
                    .map(Arc::new)
                    .map(|object_store| Self {
                        object_store,
                        schema_registry: value.schema_registry,
                    })
                    .map_err(Into::into)
            }

            _unsupported => Err(Error::UnsupportedLakeHouseUrl(crate::redact_url(
                &value.location,
            ))),
        }
    }
}

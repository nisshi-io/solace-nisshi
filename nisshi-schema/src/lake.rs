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

//! Data Lake: Delta, Iceberg or Parquet

use crate::{METER, Result};
use async_trait::async_trait;
use opentelemetry::{KeyValue, metrics::Histogram};

use nisshi_sans_io::{describe_configs_response::DescribeConfigsResult, record::inflated::Batch};
#[cfg_attr(
    not(any(feature = "parquet", feature = "iceberg", feature = "delta")),
    allow(unused_imports)
)]
use std::{fmt::Debug, marker::PhantomData, sync::LazyLock, time::SystemTime};
use tracing::instrument;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use url::Url;

#[cfg(feature = "iceberg")]
pub mod berg;

#[cfg(feature = "delta")]
pub mod delta;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
pub mod quet;

/// House
///
/// Wrapper enum for the each Data Lake implementation
#[cfg_attr(
    not(any(feature = "parquet", feature = "iceberg", feature = "delta")),
    allow(missing_copy_implementations)
)]
#[derive(Clone, Debug, Default)]
pub enum House {
    #[default]
    None,

    #[cfg(feature = "delta")]
    Delta(delta::Delta),

    #[cfg(feature = "iceberg")]
    Iceberg(berg::Iceberg),

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    Parquet(quet::Parquet),
}

/// Lake House Type
///
/// While Parquet is a common format used by both Delta and Iceberg,
/// there are some minor differences.
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LakeHouseType {
    #[cfg(feature = "delta")]
    Delta,

    #[cfg(feature = "iceberg")]
    Iceberg,

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    Parquet,

    #[default]
    None,
}

impl From<&House> for LakeHouseType {
    fn from(house: &House) -> Self {
        match house {
            #[cfg(feature = "delta")]
            House::Delta(_) => Self::Delta,

            #[cfg(feature = "iceberg")]
            House::Iceberg(_) => Self::Iceberg,

            #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
            House::Parquet(_) => Self::Parquet,

            House::None => Self::None,
        }
    }
}

impl LakeHouseType {
    #[cfg(feature = "delta")]
    pub fn is_delta(&self) -> bool {
        matches!(self, Self::Delta)
    }

    #[cfg(not(feature = "delta"))]
    pub fn is_delta(&self) -> bool {
        false
    }

    #[cfg(feature = "iceberg")]
    pub fn is_iceberg(&self) -> bool {
        matches!(self, Self::Iceberg)
    }

    #[cfg(not(feature = "iceberg"))]
    pub fn is_iceberg(&self) -> bool {
        false
    }

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    pub fn is_parquet(&self) -> bool {
        matches!(self, Self::Parquet)
    }

    #[cfg(not(any(feature = "parquet", feature = "iceberg", feature = "delta")))]
    pub fn is_parquet(&self) -> bool {
        false
    }
}

/// Lake House
///
/// This trait is implemented by [`delta::Delta`], [`berg::Iceberg`] and [`quet::Parquet`].
#[async_trait]
pub trait LakeHouse: Clone + Debug + Send + Sync + 'static {
    /// Store a batch of records in this lake house
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        inflated: &Batch,
        config: DescribeConfigsResult,
    ) -> Result<()>;

    /// Run periodic maintenance on this lake house
    async fn maintain(&self) -> Result<()>;

    /// Query the underlying type of this lake house
    async fn lake_type(&self) -> Result<LakeHouseType>;
}

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
pub(crate) static AS_ARROW_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("registry_as_arrow_duration")
        .with_unit("ms")
        .with_description("The registry as Apache Arrow latencies in milliseconds")
        .build()
});

static STORE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("lakehouse_store_duration")
        .with_unit("ms")
        .with_description("The Lake House store latencies in milliseconds")
        .build()
});

static MAINTENANCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("lakehouse_maintenance_duration")
        .with_unit("ms")
        .with_description("The Lake House maintenance latencies in milliseconds")
        .build()
});

#[async_trait]
impl LakeHouse for House {
    #[instrument(skip(self, inflated), ret)]
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        inflated: &Batch,
        configs: DescribeConfigsResult,
    ) -> Result<()> {
        let _ = (topic, partition, offset, inflated, configs.clone());

        let start = SystemTime::now();

        match self {
            #[cfg(feature = "delta")]
            House::Delta(inner) => {
                inner
                    .store(topic, partition, offset, inflated, configs)
                    .await
            }

            #[cfg(feature = "iceberg")]
            House::Iceberg(inner) => {
                inner
                    .store(topic, partition, offset, inflated, configs)
                    .await
            }

            #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
            House::Parquet(inner) => {
                inner
                    .store(topic, partition, offset, inflated, configs)
                    .await
            }

            House::None => Ok(()),
        }
        .inspect(|_| {
            STORE_DURATION.record(
                start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &[KeyValue::new("topic", topic.to_owned())],
            )
        })
    }

    #[instrument(skip(self), ret)]
    async fn maintain(&self) -> Result<()> {
        let start = SystemTime::now();

        match self {
            #[cfg(feature = "delta")]
            House::Delta(inner) => inner.maintain().await,

            #[cfg(feature = "iceberg")]
            House::Iceberg(inner) => inner.maintain().await,

            #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
            House::Parquet(inner) => inner.maintain().await,

            House::None => Ok(()),
        }
        .inspect(|_| {
            MAINTENANCE_DURATION.record(
                start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &[],
            )
        })
    }

    #[instrument(skip(self), ret)]
    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::from(self))
    }
}

impl House {
    #[cfg(feature = "iceberg")]
    pub fn iceberg() -> berg::Builder {
        berg::Builder::default()
    }

    #[cfg(feature = "delta")]
    pub fn delta() -> delta::Builder {
        delta::Builder::default()
    }

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    pub fn parquet() -> quet::Builder<PhantomData<Url>> {
        quet::Builder::default()
    }
}

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

use std::{
    convert::Infallible,
    fmt::{self, Debug},
};

use nisshi_sans_io::{
    produce_response::PartitionProduceResponse,
    record::{deflated, inflated},
};
use nisshi_schema::Registry;
use rama::{Layer, Service};
use tracing::{debug, instrument};

use super::{partition::PartitionProduce, response};

/// A [`Layer`] that validates the records of a batch against the schema of the topic in
/// `schemas`, and gives its inner [`Service`] the batch.
#[derive(Clone, Debug, Default)]
pub struct SchemaValidationLayer {
    schemas: Option<Registry>,
}

impl SchemaValidationLayer {
    /// Returns a layer that validates against the schemas in `schemas`. A layer without `schemas`
    /// does not validate.
    pub fn new(schemas: Option<Registry>) -> Self {
        Self { schemas }
    }
}

impl<S> Layer<S> for SchemaValidationLayer {
    type Service = SchemaValidationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SchemaValidationService {
            schemas: self.schemas.clone(),
            inner,
        }
    }
}

/// A [`Service`] passing a batch to `inner` when its records match the schema of the topic, or
/// answering the partition with the error code of the failed validation.
///
/// The service passes the batch without validation when it has no schemas, or when the topic's
/// `schema.validation` is `false`. A topic that has no schema in the registry has nothing to
/// validate against.
#[derive(Clone)]
pub struct SchemaValidationService<S> {
    schemas: Option<Registry>,
    inner: S,
}

impl<S> Debug for SchemaValidationService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(SchemaValidationService))
            .field("schemas", &self.schemas)
            .finish()
    }
}

impl<S> Service<PartitionProduce<(deflated::Batch, inflated::Batch)>> for SchemaValidationService<S>
where
    S: Service<
            PartitionProduce<deflated::Batch>,
            Output = PartitionProduceResponse,
            Error = Infallible,
        >,
{
    type Output = PartitionProduceResponse;
    type Error = Infallible;

    #[instrument(skip_all)]
    async fn serve(
        &self,
        request: PartitionProduce<(deflated::Batch, inflated::Batch)>,
    ) -> Result<Self::Output, Self::Error> {
        let (request, (batch, inflated)) = request.take_batch();

        // The batch is validated here, on the records that the timestamp step decoded, and before
        // storage moves the high watermark, so that a batch rejected here leaves no state behind.
        // Storage does not validate the schema, because it would decode the batch a second time.
        if let Some(schemas) = &self.schemas
            && request.configuration.schema_validation()
            && let Err(error) = schemas
                .validate(&request.topic, &inflated)
                .await
                .inspect(|validation| debug!(?validation))
                .inspect_err(|err| debug!(?err))
        {
            return Ok(response::produce_failure(
                &request.topic,
                request.index,
                batch.producer_id,
                batch.producer_epoch,
                batch.base_sequence,
                error.into(),
            ));
        }

        // The decoded batch is not needed after validation, so storage does not hold its memory.
        drop(inflated);

        self.inner.serve(request.with_batch(batch)).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use nisshi_sans_io::{
        ErrorCode,
        record::{Record, inflated},
        topic_config,
    };
    use url::Url;

    use super::*;
    use crate::{
        Result,
        service::produce::test_support::{RecordingPartitions, error_code, partition_produce},
    };

    fn person_schemas() -> Result<Registry> {
        Registry::builder_try_from_url(&Url::parse("file://../etc/schema")?)
            .map(|builder| builder.build())
            .map_err(Into::into)
    }

    /// A batch of one person of `age`, with the same batch decoded. The person schema rejects a
    /// negative age.
    fn person(age: i64) -> Result<(deflated::Batch, inflated::Batch)> {
        let deflated = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"\"345-67-6543\"")))
                    .value(Some(Bytes::from(format!(
                        r#"{{"firstName":"John","lastName":"Doe","age":{age}}}"#
                    )))),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let inflated = inflated::Batch::try_from(&deflated)?;

        Ok((deflated, inflated))
    }

    #[tokio::test]
    async fn a_batch_that_matches_the_schema_is_passed_on_without_its_decoded_records() -> Result<()>
    {
        let inner = RecordingPartitions::<deflated::Batch>::default();
        let (deflated, inflated) = person(21)?;

        let Ok(response) = SchemaValidationLayer::new(Some(person_schemas()?))
            .layer(inner.clone())
            .serve(partition_produce((deflated.clone(), inflated), &[]))
            .await;

        assert_eq!(ErrorCode::None, error_code(&response));
        assert_eq!(vec![deflated], inner.served());

        Ok(())
    }

    #[tokio::test]
    async fn a_batch_that_breaks_the_schema_is_an_invalid_record_and_is_not_passed_on() -> Result<()>
    {
        let inner = RecordingPartitions::<deflated::Batch>::default();

        let Ok(response) = SchemaValidationLayer::new(Some(person_schemas()?))
            .layer(inner.clone())
            .serve(partition_produce(person(-1)?, &[]))
            .await;

        assert_eq!(ErrorCode::InvalidRecord, error_code(&response));
        assert!(inner.served().is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn a_topic_with_schema_validation_off_passes_a_batch_that_breaks_the_schema() -> Result<()>
    {
        let inner = RecordingPartitions::<deflated::Batch>::default();

        let Ok(response) = SchemaValidationLayer::new(Some(person_schemas()?))
            .layer(inner.clone())
            .serve(partition_produce(
                person(-1)?,
                &[(topic_config::SCHEMA_VALIDATION, "false")],
            ))
            .await;

        assert_eq!(ErrorCode::None, error_code(&response));
        assert_eq!(1, inner.served().len());

        Ok(())
    }

    #[tokio::test]
    async fn a_layer_without_schemas_passes_a_batch_that_breaks_the_schema() -> Result<()> {
        let inner = RecordingPartitions::<deflated::Batch>::default();

        let Ok(response) = SchemaValidationLayer::new(None)
            .layer(inner.clone())
            .serve(partition_produce(person(-1)?, &[]))
            .await;

        assert_eq!(ErrorCode::None, error_code(&response));
        assert_eq!(1, inner.served().len());

        Ok(())
    }
}

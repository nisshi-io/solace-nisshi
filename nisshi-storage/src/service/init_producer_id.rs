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

use nisshi_sans_io::{
    ApiKey, ErrorCode, InitProducerIdRequest, InitProducerIdResponse, RequestInput,
};
use rama::Service;
use tracing::{error, info, instrument, warn};

use crate::{Error, ProducerIdResponse, Result, Storage, producer_claim};

/// A [`Service`] using its [`Storage`] taking [`InitProducerIdRequest`] returning [`InitProducerIdResponse`].
///
/// The service serves only idempotent producers, and answers a request that carries a transactional
/// ID with [`ErrorCode::TransactionalIdAuthorizationFailed`] before it calls the storage.
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{ErrorCode, InitProducerIdRequest, InitProducerIdResponse};
/// use nisshi_storage::{Error, InitProducerIdService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = InitProducerIdService { storage };
///
/// let transactional_id = None;
/// let transaction_timeout_ms = 0;
/// let producer_id = Some(-1);
/// let producer_epoch = Some(-1);
///
/// assert_eq!(
///     service
///         .serve(
///             InitProducerIdRequest::default()
///                 .transactional_id(transactional_id.clone())
///                 .transaction_timeout_ms(transaction_timeout_ms)
///                 .producer_id(producer_id)
///                 .producer_epoch(producer_epoch)
///         )
///         .await?,
///     InitProducerIdResponse::default()
///         .error_code(ErrorCode::None.into())
///         .producer_id(1)
///         .producer_epoch(0)
/// );
///
/// assert_eq!(
///     service
///         .serve(
///             InitProducerIdRequest::default()
///                 .transactional_id(transactional_id)
///                 .transaction_timeout_ms(transaction_timeout_ms)
///                 .producer_id(producer_id)
///                 .producer_epoch(producer_epoch)
///         )
///         .await?,
///     InitProducerIdResponse::default()
///         .error_code(ErrorCode::None.into())
///         .producer_id(2)
///         .producer_epoch(0)
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct InitProducerIdService<G> {
    pub storage: G,
}

impl<G> ApiKey for InitProducerIdService<G> {
    const KEY: i16 = InitProducerIdRequest::KEY;
}

impl<G, I> Service<I> for InitProducerIdService<G>
where
    G: Storage,
    I: Into<RequestInput<InitProducerIdRequest>> + Send + 'static,
{
    type Output = InitProducerIdResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        // Nisshi does not support transactions.
        let response = if input.request.transactional_id.is_some() {
            // Kafka answers this error to a principal that may not use a transactional ID
            // ([KafkaApis.scala#L2310-L2312]). The Java client's `initTransactions()` throws it
            // ([TransactionManager.java#L1325-L1329]). librdkafka treats it as fatal
            // ([rdkafka_idempotence.c#L162-L165]). In both clients, the producer stops before it
            // sends a record.
            //
            // [KafkaApis.scala#L2310-L2312]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L2310-L2312
            // [TransactionManager.java#L1325-L1329]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L1325-L1329
            // [rdkafka_idempotence.c#L162-L165]: https://github.com/confluentinc/librdkafka/blob/v2.3.0/src/rdkafka_idempotence.c#L162-L165
            warn!(
                transactional_id = ?input.request.transactional_id,
                "refusing InitProducerId: transactions are not supported"
            );
            ProducerIdResponse::failed(ErrorCode::TransactionalIdAuthorizationFailed)
        } else {
            match producer_claim(input.request.producer_id, input.request.producer_epoch) {
                Ok(claim) => {
                    // v0-2 have no ProducerId or ProducerEpoch, so a fresh request reaches storage
                    // as -1, the default Kafka gives those fields.
                    let (producer_id, producer_epoch) = claim.unwrap_or((-1, -1));

                    // A failed storage answers a code instead of failing the request, which
                    // closes the connection. A transient failure answers
                    // `COORDINATOR_LOAD_IN_PROGRESS`, the code that Kafka answers while it
                    // can't hand out a producer ID yet ([ProducerIdManager.scala#L191]), which
                    // Java and librdkafka producers retry. `COORDINATOR_NOT_AVAILABLE` would
                    // not do: for a producer with no transactional ID, the Java client answers
                    // it with a coordinator lookup that has no key, and then stops
                    // ([TransactionManager.java#L1320-L1322]).
                    //
                    // [ProducerIdManager.scala#L191]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/ProducerIdManager.scala#L191
                    // [TransactionManager.java#L1320-L1322]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L1320-L1322
                    match self
                        .storage
                        .init_producer(
                            None,
                            input.request.transaction_timeout_ms,
                            Some(producer_id),
                            Some(producer_epoch),
                        )
                        .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            error!(?error, "init producer id failed");
                            ProducerIdResponse::failed(
                                error.error_code(ErrorCode::CoordinatorLoadInProgress),
                            )
                        }
                    }
                }

                Err(error) => ProducerIdResponse::failed(error),
            }
        };

        // A failed answer carries producer ID and epoch -1, so this log is the only
        // record of what the producer claimed.
        if response.error != ErrorCode::None {
            info!(
                transactional_id = input.request.transactional_id,
                producer_id = input.request.producer_id,
                producer_epoch = input.request.producer_epoch,
                error_code = ?response.error,
                "rejecting init producer id",
            );
        }

        Ok(InitProducerIdResponse::default()
            .throttle_time_ms(0)
            .error_code(response.error.into())
            .producer_id(response.id)
            .producer_epoch(response.epoch))
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::InitProducerIdRequest;

    use crate::service::counting_storage::CountingStorage;

    use super::*;

    async fn answer(error: Error) -> Result<InitProducerIdResponse> {
        InitProducerIdService {
            storage: CountingStorage::failing_init_producer(error),
        }
        .serve(
            InitProducerIdRequest::default()
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
        )
        .await
    }

    async fn code_of(error: Error) -> Result<ErrorCode> {
        let response = answer(error).await?;

        assert_eq!((-1, -1), (response.producer_id, response.producer_epoch));

        Ok(ErrorCode::try_from(response.error_code)?)
    }

    /// Java and librdkafka producers retry `COORDINATOR_LOAD_IN_PROGRESS`. A producer with no
    /// transactional ID stops on `COORDINATOR_NOT_AVAILABLE`, so it must not answer that.
    #[tokio::test]
    async fn transient_storage_error_answers_coordinator_load_in_progress() -> Result<()> {
        assert_eq!(
            ErrorCode::CoordinatorLoadInProgress,
            code_of(Error::from(std::io::Error::other("connection reset"))).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn permanent_storage_error_answers_unknown_server_error() -> Result<()> {
        assert_eq!(
            ErrorCode::UnknownServerError,
            code_of(Error::Message("no producer ID".into())).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn storage_error_with_a_code_keeps_that_code() -> Result<()> {
        assert_eq!(
            ErrorCode::ProducerFenced,
            code_of(Error::Api(ErrorCode::ProducerFenced)).await?
        );

        Ok(())
    }
}

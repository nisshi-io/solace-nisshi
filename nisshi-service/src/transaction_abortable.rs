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

use std::fmt;

use nisshi_sans_io::{Body, ErrorCode, Frame, FrameInput, ProduceResponse};
use rama::{Layer, Service};
use tracing::{debug, instrument};

/// The first Produce version whose clients understand `TRANSACTION_ABORTABLE`.
const FIRST_TRANSACTION_ABORTABLE_PRODUCE_VERSION: i16 = 11;

/// A [`Layer`] for the Produce route that answers `INVALID_TXN_STATE` in place of
/// `TRANSACTION_ABORTABLE` to a client that sent Produce v10 or older.
///
/// Produce v11 added `TRANSACTION_ABORTABLE`. Kafka answers it to a v11 client whose
/// transactional batch goes to a partition that is not in the transaction, and
/// `INVALID_TXN_STATE` to an older client, which does not know the newer code
/// ([AddPartitionsToTxnManager.java#L98-L106], [AddPartitionsToTxnManager.java#L176-L178]).
/// librdkafka, for example, sends at most Produce v10.
///
/// The storage engines answer `TRANSACTION_ABORTABLE`, and only the Produce route knows the
/// request's version, so the downgrade happens here. It reads the version from the request's
/// frame and changes nothing else.
///
/// [AddPartitionsToTxnManager.java#L98-L106]: https://github.com/apache/kafka/blob/4.1.0/server/src/main/java/org/apache/kafka/server/transaction/AddPartitionsToTxnManager.java#L98-L106
/// [AddPartitionsToTxnManager.java#L176-L178]: https://github.com/apache/kafka/blob/4.1.0/server/src/main/java/org/apache/kafka/server/transaction/AddPartitionsToTxnManager.java#L176-L178
#[derive(Clone, Copy, Debug, Default)]
pub struct TransactionAbortableDowngradeLayer;

impl<S> Layer<S> for TransactionAbortableDowngradeLayer {
    type Service = TransactionAbortableDowngradeService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TransactionAbortableDowngradeService { inner }
    }
}

/// A [`Service`] built by [`TransactionAbortableDowngradeLayer`].
#[derive(Clone)]
pub struct TransactionAbortableDowngradeService<S> {
    inner: S,
}

impl<S> fmt::Debug for TransactionAbortableDowngradeService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(TransactionAbortableDowngradeService))
            .finish()
    }
}

impl<S> Service<FrameInput> for TransactionAbortableDowngradeService<S>
where
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<nisshi_sans_io::Error>,
{
    type Output = Frame;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let api_version = req.frame.api_version()?;

        let mut frame = self.inner.serve(req).await?;

        if api_version < FIRST_TRANSACTION_ABORTABLE_PRODUCE_VERSION
            && let Body::ProduceResponse(ref mut response) = frame.body
        {
            downgrade(response, api_version);
        }

        Ok(frame)
    }
}

fn downgrade(response: &mut ProduceResponse, api_version: i16) {
    let transaction_abortable = i16::from(ErrorCode::TransactionAbortable);

    for topic in response.responses.iter_mut().flatten() {
        for partition in topic.partition_responses.iter_mut().flatten() {
            if partition.error_code == transaction_abortable {
                debug!(
                    topic = topic.name,
                    partition = partition.index,
                    api_version,
                    "answering INVALID_TXN_STATE in place of TRANSACTION_ABORTABLE"
                );

                partition.error_code = ErrorCode::InvalidTxnState.into();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{
        ApiKey as _, Header, ProduceRequest,
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    };
    use rama::extensions::Extensions;

    use super::*;
    use crate::Error;

    fn produce_response(error_codes: &[ErrorCode]) -> ProduceResponse {
        ProduceResponse::default().responses(Some(vec![
            TopicProduceResponse::default()
                .name("abc".into())
                .partition_responses(Some(
                    error_codes
                        .iter()
                        .enumerate()
                        .map(|(index, error_code)| {
                            PartitionProduceResponse::default()
                                .index(i32::try_from(index).expect("a small index"))
                                .error_code((*error_code).into())
                        })
                        .collect(),
                )),
        ]))
    }

    fn error_codes(frame: &Frame) -> Vec<i16> {
        let Body::ProduceResponse(ref response) = frame.body else {
            panic!("a produce response: {frame:?}");
        };

        response
            .responses
            .iter()
            .flatten()
            .flat_map(|topic| topic.partition_responses.iter().flatten())
            .map(|partition| partition.error_code)
            .collect()
    }

    /// A Produce handler that answers each partition with one of `self.0`.
    struct Answer(&'static [ErrorCode]);

    impl Service<FrameInput> for Answer {
        type Output = Frame;
        type Error = Error;

        async fn serve(&self, _req: FrameInput) -> Result<Self::Output, Self::Error> {
            Ok(Frame {
                size: 0,
                header: Header::Response { correlation_id: 6 },
                body: produce_response(self.0).into(),
            })
        }
    }

    /// Serves a Produce request frame at `api_version` through the layer, to a handler that
    /// answers each partition with one of `answered`.
    async fn serve(api_version: i16, answered: &'static [ErrorCode]) -> Result<Frame, Error> {
        let service = TransactionAbortableDowngradeLayer.layer(Answer(answered));

        service
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: ProduceRequest::KEY,
                        api_version,
                        correlation_id: 6,
                        client_id: None,
                    },
                    body: ProduceRequest::default().into(),
                },
                extensions: Extensions::default(),
            })
            .await
    }

    const ANSWERED: &[ErrorCode] = &[
        ErrorCode::None,
        ErrorCode::TransactionAbortable,
        ErrorCode::NotEnoughReplicas,
    ];

    #[tokio::test]
    async fn an_older_client_gets_invalid_txn_state() -> Result<(), Error> {
        for api_version in 3..=10 {
            assert_eq!(
                vec![
                    i16::from(ErrorCode::None),
                    i16::from(ErrorCode::InvalidTxnState),
                    i16::from(ErrorCode::NotEnoughReplicas),
                ],
                error_codes(&serve(api_version, ANSWERED).await?),
                "v{api_version}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn a_v11_client_gets_transaction_abortable() -> Result<(), Error> {
        assert_eq!(
            ANSWERED.iter().map(i16::from).collect::<Vec<_>>(),
            error_codes(&serve(11, ANSWERED).await?)
        );

        Ok(())
    }
}

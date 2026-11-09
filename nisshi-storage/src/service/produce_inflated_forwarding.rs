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

//! A [`Storage`] that wraps another must forward [`Storage::produce_inflated`]. The default
//! method drops the decoded batch and calls [`Storage::produce`], so a wrapper that forgets it
//! still produces correctly, and the backend under it decodes the batch a second time. These
//! tests pin each wrapper, because nothing else would notice.

use nisshi_sans_io::record::{deflated, inflated};
use rama::{Layer as _, Service as _};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::task::{JoinSet, yield_now};
use tokio_util::sync::CancellationToken;

use crate::{
    ChannelRequestLayer, LatencyIntroducingStorage, ProduceRequestBatcher, RequestChannelService,
    RequestStorageService, Result, SemaphoreProxy, Storage, Topition, bounded_channel,
    decoded_batch_probe::probe,
    service::{
        ChannelRequest, Request, Response,
        counting_storage::{CountingStorage, Listing},
    },
};

/// Produces through `wrapper` with the decoded batch, and returns what `storage` got.
async fn through(wrapper: impl Storage, storage: &CountingStorage) -> Result<()> {
    let deflated = deflated::Batch::default();
    let inflated = inflated::Batch::default();
    let topition = Topition::new("abc", 0);

    let base_offset = wrapper
        .produce_inflated(
            Some("transaction"),
            &topition,
            deflated.clone(),
            inflated.clone(),
        )
        .await?;
    assert_eq!(0, base_offset);

    let produced = storage.produced();
    assert_eq!(1, produced.len());

    // A produce that loses its transaction id is stored as a non-transactional batch, which a
    // `read_committed` consumer reads before the transaction commits.
    assert_eq!(
        Some("transaction"),
        produced[0].transaction_id.as_deref(),
        "the wrapper did not forward the transaction id"
    );
    assert_eq!(
        topition, produced[0].topition,
        "the wrapper did not forward the topition"
    );
    assert_eq!(deflated, produced[0].deflated);
    assert_eq!(
        Some(inflated),
        produced[0].inflated,
        "the wrapper did not forward the decoded batch"
    );

    Ok(())
}

fn storage() -> CountingStorage {
    CountingStorage::new(Listing::Topics, &["abc"])
}

#[tokio::test]
async fn arc_forwards_the_decoded_batch() -> Result<()> {
    let storage = storage();

    through(Arc::new(storage.clone()), &storage).await
}

#[tokio::test]
async fn box_forwards_the_decoded_batch() -> Result<()> {
    let storage = storage();

    through(Box::new(storage.clone()), &storage).await
}

#[tokio::test]
async fn latency_storage_forwards_the_decoded_batch() -> Result<()> {
    let storage = storage();

    through(
        LatencyIntroducingStorage::new(storage.clone()).with_latency(0..1),
        &storage,
    )
    .await
}

#[tokio::test]
async fn semaphore_proxy_forwards_the_decoded_batch() -> Result<()> {
    let storage = storage();

    through(SemaphoreProxy::new(storage.clone()), &storage).await
}

#[tokio::test]
async fn request_channel_forwards_the_decoded_batch() -> Result<()> {
    let storage = storage();

    let (sender, receiver) = bounded_channel(1);
    let cancellation = CancellationToken::new();

    let mut serving = JoinSet::new();
    {
        let cancellation = cancellation.clone();
        let storage = storage.clone();

        _ = serving.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(RequestStorageService::new(storage))
                .serve(receiver)
                .await
        });
    }

    let outcome = through(RequestChannelService::new(sender), &storage).await;

    cancellation.cancel();
    _ = serving.join_all().await;

    outcome
}

/// A produce that waits for a slot in the channel holds only its compressed bytes, so a queue of
/// waiting produces does not hold a decoded batch each.
#[tokio::test]
async fn a_produce_that_waits_for_a_channel_slot_holds_no_decoded_batch() -> Result<()> {
    let (sender, mut receiver) = bounded_channel(1);
    let channel = RequestChannelService::new(sender);

    let (inflated, dropped) = probe();
    let topition = Topition::new("abc", 0);
    let waiting_topition = Topition::new("xyz", 3);

    // The first produce takes the channel's only slot, and nothing receives it yet.
    let first = channel.produce(None, &topition, deflated::Batch::default());

    let second = async {
        yield_now().await;

        channel
            .produce_inflated(
                Some("transaction"),
                &waiting_topition,
                deflated::Batch::default(),
                inflated,
            )
            .await
    };

    let serve = async {
        // Every branch has been polled once after these yields, so the second produce is waiting.
        for _ in 0..4 {
            yield_now().await;
        }
        let held = !dropped.load(Ordering::SeqCst);

        let mut sent_without = true;
        let mut received = Vec::new();
        for offset in 0..2 {
            let ChannelRequest {
                request,
                response: reply,
                ..
            } = receiver.recv().await.expect("a produce request");

            let Request::Produce {
                transaction_id,
                topition,
                inflated,
                ..
            } = request
            else {
                panic!("{request:?}");
            };
            sent_without &= inflated.is_none();
            received.push((transaction_id, topition));

            reply
                .send(Ok(Response::Produce(Ok(offset))))
                .expect("the producer");
        }

        (held, sent_without, received)
    };

    let (first, second, (held, sent_without, received)) = tokio::join!(first, second, serve);

    assert_eq!(0, first?);
    assert_eq!(1, second?);
    assert!(!held, "the waiting produce still held the decoded batch");
    assert!(
        sent_without,
        "a produce reached storage with a decoded batch"
    );

    // A waiting produce that lost its transaction id would be stored as a non-transactional
    // batch.
    assert_eq!(
        vec![
            (None, topition),
            (Some("transaction".to_owned()), waiting_topition)
        ],
        received,
        "the waiting produce did not forward its transaction id and topition"
    );

    Ok(())
}

/// `produce` without a decoded batch still reaches storage without one.
#[tokio::test]
async fn request_channel_produce_carries_no_decoded_batch() -> Result<()> {
    let storage = storage();

    let (sender, receiver) = bounded_channel(1);
    let cancellation = CancellationToken::new();

    let mut serving = JoinSet::new();
    {
        let cancellation = cancellation.clone();
        let storage = storage.clone();

        _ = serving.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(RequestStorageService::new(storage))
                .serve(receiver)
                .await
        });
    }

    _ = RequestChannelService::new(sender)
        .produce(None, &Topition::new("abc", 0), deflated::Batch::default())
        .await?;

    cancellation.cancel();
    _ = serving.join_all().await;

    let produced = storage.produced();
    assert_eq!(1, produced.len());
    assert_eq!(None, produced[0].inflated);

    Ok(())
}

#[tokio::test]
async fn batcher_without_a_delay_forwards_the_decoded_batch() -> Result<()> {
    let storage = storage();

    through(ProduceRequestBatcher::new(storage.clone()), &storage).await
}

/// With a delay the batcher holds the decoded batch in its queue. The default batch is
/// idempotent, so the flush writes it as sent, and passes the held decoded batch on.
#[tokio::test]
async fn batcher_with_a_delay_forwards_the_decoded_batch_that_it_held() -> Result<()> {
    let storage = storage();

    _ = ProduceRequestBatcher::new(storage.clone())
        .with_maximum_delay(Some(Duration::from_millis(1)))
        .produce_inflated(
            None,
            &Topition::new("abc", 0),
            deflated::Batch::default(),
            inflated::Batch::default(),
        )
        .await?;

    let produced = storage.produced();
    assert_eq!(1, produced.len());
    assert!(
        produced[0].inflated.is_some(),
        "the batcher dropped the decoded batch that it held"
    );

    Ok(())
}

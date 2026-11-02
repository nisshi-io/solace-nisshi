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
use std::{sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    ChannelRequestLayer, LatencyIntroducingStorage, ProduceRequestBatcher, RequestChannelService,
    RequestStorageService, Result, SemaphoreProxy, Storage, Topition, bounded_channel,
    service::counting_storage::{CountingStorage, Listing},
};

/// Produces through `wrapper` with the decoded batch, and returns what `storage` got.
async fn through(wrapper: impl Storage, storage: &CountingStorage) -> Result<()> {
    let deflated = deflated::Batch::default();
    let inflated = inflated::Batch::default();

    let base_offset = wrapper
        .produce_inflated(
            Some("transaction"),
            &Topition::new("abc", 0),
            deflated.clone(),
            inflated.clone(),
        )
        .await?;
    assert_eq!(0, base_offset);

    let produced = storage.produced();
    assert_eq!(1, produced.len());
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

/// With a delay the batcher combines queued batches, which decodes them again, so it drops the
/// decoded batch and produces the bytes as before.
#[tokio::test]
async fn batcher_with_a_delay_drops_the_decoded_batch() -> Result<()> {
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
    assert_eq!(None, produced[0].inflated);

    Ok(())
}

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

//! A transactional produce raises the partition's high watermark, and then
//! records the transaction's offsets in `meta.json`. The last stable offset
//! must not pass the transaction's records in between, nor after a produce
//! that fails between the two writes.

use std::{
    assert_matches,
    fmt::{self, Debug, Display},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_sans_io::{
    BatchAttribute, ErrorCode, IsolationLevel, ListOffset,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage as _, Topition, TxnAddPartitionsRequest};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::sync::{Notify, oneshot};

use crate::dynostore::{DynoStore, last_stable, tests::init_tracing};

const TRANSACTION: &str = "txn";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Hold,
    Fail,
}

/// Once armed, holds (or fails) the first `meta.json` put that follows a
/// `watermark.json` put, once, and passes everything else through.
#[derive(Clone)]
struct BetweenWritesStore {
    object_store: Arc<InMemory>,
    mode: Mode,
    armed: Arc<AtomicBool>,
    watermark_written: Arc<AtomicBool>,
    fired: Arc<AtomicUsize>,
    held: Arc<Notify>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

impl BetweenWritesStore {
    /// Starts watching for the puts. The setup before a test writes both
    /// documents too, so it runs unarmed.
    fn arm(&self) {
        self.watermark_written.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl Debug for BetweenWritesStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BetweenWritesStore").finish()
    }
}

impl Display for BetweenWritesStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BetweenWritesStore").finish()
    }
}

#[async_trait]
impl ObjectStore for BetweenWritesStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let path = location.as_ref();

        if self.armed.load(Ordering::SeqCst) {
            if path.ends_with("watermark.json") {
                self.watermark_written.store(true, Ordering::SeqCst);
            } else if path.ends_with("meta.json")
                && self.watermark_written.load(Ordering::SeqCst)
                && self.fired.fetch_add(1, Ordering::SeqCst) == 0
            {
                self.armed.store(false, Ordering::SeqCst);

                match self.mode {
                    Mode::Hold => {
                        let release = self
                            .release
                            .lock()
                            .ok()
                            .and_then(|mut release| release.take());

                        self.held.notify_one();

                        if let Some(release) = release {
                            _ = release.await;
                        }
                    }

                    Mode::Fail => {
                        return Err(object_store::Error::Generic {
                            store: "test",
                            source: "forced failure".into(),
                        });
                    }
                }
            }
        }

        self.object_store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.object_store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.object_store.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.object_store.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.object_store.copy_opts(from, to, opts).await
    }
}

fn plain_batch() -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"plain"))))
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

/// A transactional batch of 3 records.
fn transactional_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"a"))))
        .record(
            Record::builder()
                .value(Some(Bytes::from_static(b"b")))
                .offset_delta(1),
        )
        .record(
            Record::builder()
                .value(Some(Bytes::from_static(b"c")))
                .offset_delta(2),
        )
        .last_offset_delta(2)
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

async fn latest(
    storage: &DynoStore,
    isolation_level: IsolationLevel,
    topition: &Topition,
) -> Result<Option<i64>, Error> {
    let responses = storage
        .list_offsets(isolation_level, &[(topition.clone(), ListOffset::Latest)])
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(ErrorCode::None, responses[0].1.error_code);

    Ok(responses[0].1.offset)
}

struct Setup {
    storage: DynoStore,
    object_store: BetweenWritesStore,
    topition: Topition,
    producer_id: i64,
    producer_epoch: i16,
    release: oneshot::Sender<()>,
}

/// A partition holding 2 plain records, and a transaction that has added it.
async fn setup(mode: Mode) -> Result<Setup, Error> {
    let (release, release_receiver) = oneshot::channel();

    let object_store = BetweenWritesStore {
        object_store: Arc::new(InMemory::new()),
        mode,
        armed: Arc::new(AtomicBool::new(false)),
        watermark_written: Arc::new(AtomicBool::new(false)),
        fired: Arc::new(AtomicUsize::new(0)),
        held: Arc::new(Notify::new()),
        release: Arc::new(Mutex::new(Some(release_receiver))),
    };

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
    assert_eq!(0, storage.produce(None, &topition, plain_batch()?).await?);
    assert_eq!(1, storage.produce(None, &topition, plain_batch()?).await?);

    let producer = storage
        .init_producer(Some(TRANSACTION), 10_000, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, producer.error);

    _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: TRANSACTION.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name("abc".into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await?;

    assert_eq!(
        Some(2),
        latest(&storage, IsolationLevel::ReadCommitted, &topition).await?
    );

    Ok(Setup {
        storage,
        object_store,
        topition,
        producer_id: producer.id,
        producer_epoch: producer.epoch,
        release,
    })
}

#[tokio::test]
async fn read_committed_stops_at_a_transaction_between_its_two_writes() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let Setup {
        storage,
        object_store,
        topition,
        producer_id,
        producer_epoch,
        release,
    } = setup(Mode::Hold).await?;

    object_store.arm();

    let produce = storage.produce(
        Some(TRANSACTION),
        &topition,
        transactional_batch(producer_id, producer_epoch, 0)?,
    );

    let between_writes = async {
        object_store.held.notified().await;

        let read_committed = latest(&storage, IsolationLevel::ReadCommitted, &topition).await;
        let read_uncommitted = latest(&storage, IsolationLevel::ReadUncommitted, &topition).await;
        let stage = storage.offset_stage(&topition).await;

        _ = release.send(());
        (read_committed, read_uncommitted, stage)
    };

    let (produce, (read_committed, read_uncommitted, stage)) =
        tokio::join!(produce, between_writes);

    assert_eq!(2, produce?);

    // The high watermark already counts the transaction's 3 records.
    assert_eq!(Some(5), read_uncommitted?);
    let stage = stage?;
    assert_eq!(5, stage.high_watermark());

    // ListOffsets(Latest) and the last stable offset that Fetch answers both
    // stop at the transaction's first offset.
    assert_eq!(Some(2), read_committed?);
    assert_eq!(2, stage.last_stable());

    assert_eq!(
        Some(2),
        latest(&storage, IsolationLevel::ReadCommitted, &topition).await?
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(TRANSACTION, producer_id, producer_epoch, true)
            .await?
    );

    // The commit marker follows the transaction's records.
    assert_eq!(
        Some(6),
        latest(&storage, IsolationLevel::ReadCommitted, &topition).await?
    );

    Ok(())
}

#[tokio::test]
async fn failed_produce_between_its_two_writes_still_holds_and_marks_the_partition()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    let Setup {
        storage,
        object_store,
        topition,
        producer_id,
        producer_epoch,
        ..
    } = setup(Mode::Fail).await?;

    object_store.arm();

    let produce = storage
        .produce(
            Some(TRANSACTION),
            &topition,
            transactional_batch(producer_id, producer_epoch, 0)?,
        )
        .await;
    assert!(produce.is_err(), "{produce:?}");

    // The failed produce allocated offsets 2 to 4.
    let stage = storage.offset_stage(&topition).await?;
    assert_eq!(5, stage.high_watermark());
    assert_eq!(2, stage.last_stable());

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(TRANSACTION, producer_id, producer_epoch, false)
            .await?
    );

    // The abort wrote its marker to the partition, at offset 5.
    let stage = storage.offset_stage(&topition).await?;
    assert_eq!(6, stage.high_watermark());
    assert_eq!(6, stage.last_stable());

    Ok(())
}

/// A retry of a batch is stored while the first attempt is held after its
/// high watermark write. The first attempt's records are written too, below
/// the retry's, and the last stable offset stops before them.
#[tokio::test]
async fn retry_recorded_first_does_not_raise_the_transaction_start() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let Setup {
        storage,
        object_store,
        topition,
        producer_id,
        producer_epoch,
        release,
    } = setup(Mode::Hold).await?;

    object_store.arm();

    let first_attempt = storage.produce(
        Some(TRANSACTION),
        &topition,
        transactional_batch(producer_id, producer_epoch, 0)?,
    );

    let retry = async {
        object_store.held.notified().await;

        let retry = storage
            .produce(
                Some(TRANSACTION),
                &topition,
                transactional_batch(producer_id, producer_epoch, 0)?,
            )
            .await;

        let read_committed = latest(&storage, IsolationLevel::ReadCommitted, &topition).await;
        let read_uncommitted = latest(&storage, IsolationLevel::ReadUncommitted, &topition).await;

        _ = release.send(());
        Ok::<_, Error>((retry, read_committed, read_uncommitted))
    };

    let (first_attempt, retry) = tokio::join!(first_attempt, retry);
    let (retry, read_committed, read_uncommitted) = retry?;

    assert_eq!(5, retry?);
    assert_eq!(Some(8), read_uncommitted?);
    assert_eq!(Some(2), read_committed?);

    assert_matches!(
        first_attempt,
        Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
    );

    assert_eq!(
        Some(2),
        latest(&storage, IsolationLevel::ReadCommitted, &topition).await?
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(TRANSACTION, producer_id, producer_epoch, true)
            .await?
    );

    assert_eq!(
        Some(9),
        latest(&storage, IsolationLevel::ReadCommitted, &topition).await?
    );

    Ok(())
}

#[test]
fn last_stable_offset_bounds() {
    // No open transaction: the high watermark.
    assert_eq!(7, last_stable(7, 0, None));

    // An open transaction holds it back to its first offset.
    assert_eq!(3, last_stable(7, 0, Some(3)));

    // Never past the high watermark, when an open transaction was recorded
    // after the high watermark was read.
    assert_eq!(7, last_stable(7, 0, Some(9)));

    // Never below the log start.
    assert_eq!(5, last_stable(7, 5, Some(3)));
    assert_eq!(5, last_stable(5, 5, None));
}

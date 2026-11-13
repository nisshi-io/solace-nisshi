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

use std::{future::Future, sync::LazyLock, time::Duration};

use nisshi_sans_io::{Ack, ErrorCode, produce_response::PartitionProduceResponse};
use opentelemetry::{KeyValue, metrics::Counter};
use tokio::time::{Instant, error::Elapsed, timeout_at};

use super::response;
use crate::METER;

/// The longest that a Produce request that waits for an answer may take before the broker answers
/// it, when the request's `timeout_ms` allows more.
///
/// A client gives up on a request that runs past its own timeout and closes the connection. The
/// Java producer's `request.timeout.ms` is 30 s by default, and the request's `timeout_ms` is the
/// same value. librdkafka sends a `timeout_ms` of 30 s as well, and waits for 60 s
/// (`socket.timeout.ms`). Without a deadline a request answers after the sum of its partitions'
/// storage calls, so a storage backend whose produce call is slow makes a request for a few
/// partitions outlast the client. 20 s is under 30 s, with room to write the response, and above
/// the 10 s that a request waits for the decoded-batch budget.
///
/// Kafka has no such limit on the append, because the append of a partition is local and fast.
/// Its `timeout_ms` bounds only the wait for the replicas ([ReplicaManager.scala#L949-L958]), and
/// a partition that has appended answers `REQUEST_TIMED_OUT` when that wait ends
/// ([DelayedProduce.scala#L62-L72]). Nisshi has no replicas, so its answer to the same request
/// is the storage call, and the deadline bounds that.
///
/// [ReplicaManager.scala#L949-L958]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L949-L958
/// [DelayedProduce.scala#L62-L72]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DelayedProduce.scala#L62-L72
pub(super) const PRODUCE_REQUEST_DEADLINE: Duration = Duration::from_secs(20);

/// The message of a partition that the deadline stopped before its write started.
const NOT_STARTED: &str =
    "the request's deadline passed before the broker started this partition's write, retry it";

static DEADLINE_EXCEEDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_produce_deadline_exceeded")
        .with_description(
            "Produce partitions answered REQUEST_TIMED_OUT because the request's deadline passed",
        )
        .build()
});

/// How far a partition had come when the deadline stopped it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Stage {
    /// No storage write had started for the partition.
    NotStarted,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::NotStarted => NOT_STARTED,
        }
    }
}

/// When a Produce request must be answered, shared by its partitions.
///
/// A request that does not wait for an answer, `acks=0`, has none: Kafka never times it out, and
/// the service closes the connection on any partition error, so a partition error is not a
/// better outcome than waiting.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Deadline {
    at: Option<Instant>,
}

impl Deadline {
    /// Returns the deadline of a request that arrived now, with `acks` and `timeout_ms`.
    ///
    /// It is the smaller of [`PRODUCE_REQUEST_DEADLINE`] and `timeout_ms`, which is what the
    /// client's own wait is for a Java producer. A `timeout_ms` of zero or less is no limit, as
    /// librdkafka and the Java producer never send one, so the constant applies.
    pub(super) fn new(acks: Ack, timeout_ms: i32) -> Self {
        if acks == Ack::None {
            return Self::none();
        }

        let limit = u64::try_from(timeout_ms)
            .ok()
            .filter(|timeout_ms| *timeout_ms > 0)
            .map_or(PRODUCE_REQUEST_DEADLINE, Duration::from_millis)
            .min(PRODUCE_REQUEST_DEADLINE);

        Self {
            at: Some(Instant::now() + limit),
        }
    }

    /// Returns the deadline of a request that has none.
    pub(super) fn none() -> Self {
        Self { at: None }
    }

    /// Returns whether the deadline has passed.
    pub(super) fn has_passed(&self) -> bool {
        self.at.is_some_and(|at| Instant::now() >= at)
    }

    /// Runs `future` until the deadline, and drops it then. Use this only for a future that is
    /// safe to drop, such as a read.
    pub(super) async fn bound<F>(&self, future: F) -> Result<F::Output, Elapsed>
    where
        F: Future,
    {
        match self.at {
            Some(at) => timeout_at(at, future).await,
            None => Ok(future.await),
        }
    }

    /// Returns the response for the partition at `index` that the deadline stopped at `stage`,
    /// and counts it.
    ///
    /// `REQUEST_TIMED_OUT` is the code that Kafka answers a partition that appended and did not
    /// get its acks in time. A client retries it, and the Java producer sends the same sequence
    /// again. A partition at [`Stage::NotStarted`] differs from Kafka's: nothing was written.
    /// Kafka has no such state, because its append does not wait.
    pub(super) fn timed_out(stage: Stage, index: i32) -> PartitionProduceResponse {
        DEADLINE_EXCEEDED.add(1, &[KeyValue::new("stage", stage.as_str())]);

        response::partition_error(index, ErrorCode::RequestTimedOut)
            .error_message(Some(stage.message().to_owned()))
    }
}

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

use crate::common::{
    self, CLIENT_ID, COOPERATIVE_STICKY, PROTOCOL_TYPE, RANGE, StorageType, alphanumeric_string,
    join, join_group, register_broker,
};
use nisshi_broker::{Result, coordinator::group::administrator::Controller};
use nisshi_sans_io::{ErrorCode, join_group_request::JoinGroupRequestProtocol};
use nisshi_storage::Storage;
use rand::{prelude::*, rng};
use tokio::time::{Duration, timeout};
use url::Url;
use uuid::Uuid;

// Kafka's `group.max.session.timeout.ms` default. Nisshi has no equivalent config,
// so this is the boundary the fix hardcodes (see administrator.rs).
const MAX_SESSION_TIMEOUT_MS: i32 = 1_800_000;

fn protocols() -> Vec<JoinGroupRequestProtocol> {
    vec![
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(bytes::Bytes::from_static(b"range-meta")),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(bytes::Bytes::from_static(b"sticky-meta")),
    ]
}

/// A static member's very first `JoinGroup` (`group_instance_id` set) skips the
/// `MemberIdRequired` round trip and goes straight into the loop that (pre-fix) casts
/// `session_timeout_ms` to `u128` and divides by it. A negative value sign-extends to
/// ~2^128, so a fixed 5s `tokio::time::timeout` turns the pre-fix infinite loop into a
/// clean assertion failure instead of a hung test process.
///
/// Each invalid value is tried against a fresh group id, and each is followed by a
/// well-formed join to that *same* group id, proving the rejected attempt left no group
/// state behind for a later, legitimate member to inherit.
pub async fn reject_invalid_session_timeout_on_join<G>(
    cluster_id: impl Into<String> + Clone,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id.clone(), broker_id, &sc).await?;

    for invalid_session_timeout_ms in [-1, 0, MAX_SESSION_TIMEOUT_MS + 1] {
        let mut controller = Controller::with_storage(sc.clone())?;

        let group_id: String = alphanumeric_string(15);
        let group_instance_id = format!("static-{}", alphanumeric_string(8));

        let rejected = timeout(
            Duration::from_secs(5),
            join_group(
                &mut controller,
                Some(CLIENT_ID),
                group_id.as_str(),
                invalid_session_timeout_ms,
                Some(300_000),
                "",
                Some(group_instance_id.as_str()),
                PROTOCOL_TYPE,
                Some(&protocols()[..]),
                None,
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "join with session_timeout_ms={invalid_session_timeout_ms} did not return \
                 within 5s (pre-fix: unbounded wait loop)"
            )
        })?;

        assert_eq!(
            ErrorCode::InvalidSessionTimeout,
            ErrorCode::try_from(rejected.error_code)?,
            "session_timeout_ms={invalid_session_timeout_ms}"
        );
        assert_eq!(-1, rejected.generation_id);
        assert!(rejected.leader.is_empty());
        assert!(rejected.member_id.is_empty());
        assert_eq!(0, rejected.members.unwrap().len());

        // The rejected attempt must not have created or poisoned any group state: a
        // well-formed join to the same group id succeeds normally afterwards. A small
        // (but valid) session_timeout_ms keeps this fast: a lone-leader join waits up to
        // session_timeout_ms/2 before returning, regardless of validity. This is also
        // wrapped in a timeout: if a regression let the earlier, invalid attempt persist
        // its bad session_timeout_ms before being rejected, this call would be the one
        // that hangs, and it should fail cleanly rather than run until nextest's CI
        // profile kills it.
        let joined = timeout(
            Duration::from_secs(10),
            join(
                &mut controller,
                group_id.as_str(),
                None,
                None,
                Some(protocols()),
                6_000,
                Some(300_000),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "well-formed join after rejecting session_timeout_ms={invalid_session_timeout_ms} \
                 did not return within 10s (did the rejected attempt leak group state?)"
            )
        })?;

        assert!(joined.is_leader());
        assert!(!joined.id().is_empty());
    }

    Ok(())
}

/// librdkafka's own compat suite uses `session.timeout.ms` as low as 5000 (below Kafka's
/// broker-side `group.min.session.timeout.ms` default of 6000, which nisshi does not
/// enforce). 5000 must continue to be accepted, not rejected as "invalid".
pub async fn accept_boundary_session_timeout_on_join<G>(
    cluster_id: impl Into<String> + Clone,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id.clone(), broker_id, &sc).await?;

    let mut controller = Controller::with_storage(sc.clone())?;

    let group_id: String = alphanumeric_string(15);
    let group_instance_id = format!("static-{}", alphanumeric_string(8));

    let accepted = timeout(
        Duration::from_secs(10),
        join_group(
            &mut controller,
            Some(CLIENT_ID),
            group_id.as_str(),
            5_000,
            Some(300_000),
            "",
            Some(group_instance_id.as_str()),
            PROTOCOL_TYPE,
            Some(&protocols()[..]),
            None,
        ),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("well-formed join with session_timeout_ms=5000 did not return within 10s")
    })?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(accepted.error_code)?);
    assert!(!accepted.member_id.is_empty());
    assert_eq!(accepted.member_id, accepted.leader);

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn reject_invalid_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::reject_invalid_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn accept_boundary_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::accept_boundary_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn reject_invalid_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::reject_invalid_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn accept_boundary_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::accept_boundary_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Lite,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn reject_invalid_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::reject_invalid_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn accept_boundary_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::accept_boundary_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Turso,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn reject_invalid_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::reject_invalid_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn accept_boundary_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::accept_boundary_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn reject_invalid_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::reject_invalid_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn accept_boundary_session_timeout_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::accept_boundary_session_timeout_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

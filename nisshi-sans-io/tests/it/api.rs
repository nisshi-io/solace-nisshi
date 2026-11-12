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

use std::process::{ExitCode, Termination as _};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use nisshi_sans_io::{
    Ack, ApiKey as _, BatchAttribute, Body, Compression, ConfigResource, ConfigSource, ConfigType,
    ControlBatch, CoordinatorType, EndTransactionMarker, EndpointType, Error, ErrorCode,
    FetchRequest, FetchResponse, Frame, Header, IsolationLevel, ListOffset, MaximumAllocationSize,
    OpType, Result, TimestampType, to_system_time, to_timestamp,
};

#[test]
fn frame() -> Result<()> {
    let api_version = 6;
    let correlation_id = 32123;
    let client_id = "client";

    let request = Frame {
        size: Default::default(),
        header: Header::Request {
            api_key: FetchRequest::KEY,
            api_version,
            correlation_id,
            client_id: Some(client_id.into()),
        },
        body: Body::FetchRequest(FetchRequest::default()),
    };

    assert_eq!(FetchRequest::KEY, request.api_key()?);
    assert_eq!(api_version, request.api_version()?);
    assert_eq!("FetchRequest", request.api_name());
    assert_eq!(correlation_id, request.correlation_id()?);
    assert_eq!(Some(client_id), request.client_id()?);

    let response = Frame {
        size: Default::default(),
        header: Header::Response { correlation_id },
        body: Body::FetchResponse(FetchResponse::default()),
    };

    assert!(response.api_key().is_err());
    assert!(response.api_version().is_err());
    assert_eq!("FetchResponse", response.api_name());
    assert_eq!(correlation_id, response.correlation_id()?);
    assert!(response.client_id().is_err());

    Ok(())
}

#[test]
fn error_code() -> Result<()> {
    for error_code in -1..120 {
        assert_eq!(error_code, ErrorCode::try_from(error_code).map(i16::from)?);
    }

    Ok(())
}

#[test]
fn termination() {
    assert_eq!(ExitCode::SUCCESS, ErrorCode::None.report());
    assert_eq!(ExitCode::FAILURE, ErrorCode::UnknownMemberId.report());
}

#[test]
fn isolation_level() -> Result<()> {
    const READ_UNCOMMITTED: i8 = 0;

    assert_eq!(
        READ_UNCOMMITTED,
        IsolationLevel::try_from(READ_UNCOMMITTED).map(i8::from)?
    );

    const READ_COMMITTED: i8 = 1;

    assert_eq!(
        READ_COMMITTED,
        IsolationLevel::try_from(READ_COMMITTED).map(i8::from)?
    );

    Ok(())
}

#[test]
fn ack() -> Result<()> {
    const FULL_ISR: i16 = -1;
    const NONE: i16 = 0;
    const LEADER: i16 = 1;

    assert_eq!(FULL_ISR, Ack::try_from(FULL_ISR).map(i16::from)?);
    assert_eq!(NONE, Ack::try_from(NONE).map(i16::from)?);
    assert_eq!(LEADER, Ack::try_from(LEADER).map(i16::from)?);

    Ok(())
}

#[test]
fn timestamp_type() {
    const CREATE_TIME: i16 = 0;
    const LOG_APPEND_TIME: i16 = 8;

    assert_eq!(CREATE_TIME, i16::from(TimestampType::from(CREATE_TIME)));
    assert_eq!(
        LOG_APPEND_TIME,
        i16::from(TimestampType::from(LOG_APPEND_TIME))
    );

    assert_eq!(TimestampType::CreateTime, TimestampType::from(CREATE_TIME));
    assert_eq!(
        TimestampType::LogAppendTime,
        TimestampType::from(LOG_APPEND_TIME)
    );
}

#[test]
fn compression() -> Result<()> {
    const NONE: i16 = 0;
    const GZIP: i16 = 1;
    const SNAPPY: i16 = 2;
    const LZ4: i16 = 3;
    const ZSTD: i16 = 4;

    assert_eq!(NONE, Compression::try_from(NONE).map(i16::from)?);
    assert_eq!(GZIP, Compression::try_from(GZIP).map(i16::from)?);
    assert_eq!(SNAPPY, Compression::try_from(SNAPPY).map(i16::from)?);
    assert_eq!(LZ4, Compression::try_from(LZ4).map(i16::from)?);
    assert_eq!(ZSTD, Compression::try_from(ZSTD).map(i16::from)?);

    assert!(Compression::try_from(5i16).is_err());

    Ok(())
}

#[test]
fn batch_attribute() -> Result<()> {
    // default round-trips to 0
    assert_eq!(
        BatchAttribute::default(),
        BatchAttribute::try_from(i16::from(BatchAttribute::default()))?
    );

    // compression round-trip
    for value in 0i16..=4 {
        let attr = BatchAttribute::try_from(value)?;
        assert_eq!(value, i16::from(attr));
    }

    // timestamp bit
    let attr = BatchAttribute::try_from(8i16)?;
    assert_eq!(TimestampType::LogAppendTime, attr.timestamp);

    // transaction bit
    let attr = BatchAttribute::try_from(16i16)?;
    assert!(attr.transaction);

    // control bit
    let attr = BatchAttribute::try_from(32i16)?;
    assert!(attr.control);

    // delete horizon bit
    let attr = BatchAttribute::try_from(64i16)?;
    assert!(attr.delete_horizon);

    // combined: gzip + log_append_time + transaction + control + delete_horizon
    let combined: i16 = 1 | 8 | 16 | 32 | 64;
    let attr = BatchAttribute::try_from(combined)?;
    assert_eq!(combined, i16::from(attr));

    Ok(())
}

#[test]
fn control_batch() -> Result<()> {
    let commit = ControlBatch::default().commit();
    assert!(commit.is_commit());
    assert!(!commit.is_abort());

    let abort = ControlBatch::default().abort();
    assert!(abort.is_abort());
    assert!(!abort.is_commit());

    // round-trip through Bytes
    let commit = ControlBatch::default().version(1).commit();
    let encoded = Bytes::try_from(commit.clone())?;
    let decoded = ControlBatch::try_from(encoded)?;
    assert_eq!(commit, decoded);

    let abort = ControlBatch::default().version(1).abort();
    let encoded = Bytes::try_from(abort.clone())?;
    let decoded = ControlBatch::try_from(encoded)?;
    assert_eq!(abort, decoded);

    Ok(())
}

#[test]
fn end_transaction_marker() -> Result<()> {
    let marker = EndTransactionMarker {
        version: 1,
        coordinator_epoch: 42,
    };

    let encoded = Bytes::try_from(marker.clone())?;
    let decoded = EndTransactionMarker::try_from(encoded)?;
    assert_eq!(marker, decoded);

    Ok(())
}

#[test]
fn endpoint_type() {
    const UNKNOWN: i8 = 0;
    const BROKER: i8 = 1;
    const CONTROLLER: i8 = 2;

    assert_eq!(UNKNOWN, i8::from(EndpointType::from(UNKNOWN)));
    assert_eq!(BROKER, i8::from(EndpointType::from(BROKER)));
    assert_eq!(CONTROLLER, i8::from(EndpointType::from(CONTROLLER)));

    // unrecognized values map to Unknown
    assert_eq!(UNKNOWN, i8::from(EndpointType::from(99)));
}

#[test]
fn coordinator_type() -> Result<()> {
    const GROUP: i8 = 0;
    const TRANSACTION: i8 = 1;
    const SHARE: i8 = 2;

    assert_eq!(GROUP, CoordinatorType::try_from(GROUP).map(i8::from)?);
    assert_eq!(
        TRANSACTION,
        CoordinatorType::try_from(TRANSACTION).map(i8::from)?
    );
    assert_eq!(SHARE, CoordinatorType::try_from(SHARE).map(i8::from)?);

    assert!(CoordinatorType::try_from(3i8).is_err());

    Ok(())
}

#[test]
fn config_resource() {
    const TOPIC: i8 = 2;
    const BROKER: i8 = 4;
    const BROKER_LOGGER: i8 = 8;
    const CLIENT_METRIC: i8 = 16;
    const GROUP: i8 = 32;

    assert_eq!(TOPIC, i8::from(ConfigResource::from(TOPIC)));
    assert_eq!(BROKER, i8::from(ConfigResource::from(BROKER)));
    assert_eq!(BROKER_LOGGER, i8::from(ConfigResource::from(BROKER_LOGGER)));
    assert_eq!(CLIENT_METRIC, i8::from(ConfigResource::from(CLIENT_METRIC)));
    assert_eq!(GROUP, i8::from(ConfigResource::from(GROUP)));

    // unrecognized values map to Unknown
    assert_eq!(0i8, i8::from(ConfigResource::from(99)));

    // i32 conversion
    assert_eq!(2i32, i32::from(ConfigResource::Topic));
    assert_eq!(4i32, i32::from(ConfigResource::Broker));
}

#[test]
fn config_source() {
    const DYNAMIC_TOPIC_CONFIG: i8 = 1;
    const DYNAMIC_BROKER_CONFIG: i8 = 2;
    const DYNAMIC_DEFAULT_BROKER_CONFIG: i8 = 3;
    const STATIC_BROKER_CONFIG: i8 = 4;
    const DEFAULT_CONFIG: i8 = 5;
    const DYNAMIC_BROKER_LOGGER_CONFIG: i8 = 6;
    const DYNAMIC_CLIENT_METRICS_CONFIG: i8 = 7;
    const DYNAMIC_GROUP_CONFIG: i8 = 8;

    assert_eq!(
        DYNAMIC_TOPIC_CONFIG,
        i8::from(ConfigSource::from(DYNAMIC_TOPIC_CONFIG))
    );
    assert_eq!(
        DYNAMIC_BROKER_CONFIG,
        i8::from(ConfigSource::from(DYNAMIC_BROKER_CONFIG))
    );
    assert_eq!(
        DYNAMIC_DEFAULT_BROKER_CONFIG,
        i8::from(ConfigSource::from(DYNAMIC_DEFAULT_BROKER_CONFIG))
    );
    assert_eq!(
        STATIC_BROKER_CONFIG,
        i8::from(ConfigSource::from(STATIC_BROKER_CONFIG))
    );
    assert_eq!(DEFAULT_CONFIG, i8::from(ConfigSource::from(DEFAULT_CONFIG)));
    assert_eq!(
        DYNAMIC_BROKER_LOGGER_CONFIG,
        i8::from(ConfigSource::from(DYNAMIC_BROKER_LOGGER_CONFIG))
    );
    assert_eq!(
        DYNAMIC_CLIENT_METRICS_CONFIG,
        i8::from(ConfigSource::from(DYNAMIC_CLIENT_METRICS_CONFIG))
    );
    assert_eq!(
        DYNAMIC_GROUP_CONFIG,
        i8::from(ConfigSource::from(DYNAMIC_GROUP_CONFIG))
    );

    // unrecognized values map to Unknown
    assert_eq!(0i8, i8::from(ConfigSource::from(99)));
}

#[test]
fn op_type() -> Result<()> {
    const SET: i8 = 0;
    const DELETE: i8 = 1;
    const APPEND: i8 = 2;
    const SUBTRACT: i8 = 3;

    assert_eq!(SET, OpType::try_from(SET).map(i8::from)?);
    assert_eq!(DELETE, OpType::try_from(DELETE).map(i8::from)?);
    assert_eq!(APPEND, OpType::try_from(APPEND).map(i8::from)?);
    assert_eq!(SUBTRACT, OpType::try_from(SUBTRACT).map(i8::from)?);

    assert!(OpType::try_from(4i8).is_err());

    Ok(())
}

#[test]
fn system_time_and_timestamp() -> Result<()> {
    // epoch converts to 0 and back
    let epoch_millis: i64 = 0;
    let system_time = to_system_time(epoch_millis)?;
    assert_eq!(SystemTime::UNIX_EPOCH, system_time);
    assert_eq!(epoch_millis, to_timestamp(&system_time)?);

    // a known timestamp round-trips
    let millis: i64 = 1_700_000_000_000;
    let system_time = to_system_time(millis)?;
    assert_eq!(
        SystemTime::UNIX_EPOCH + Duration::from_millis(millis as u64),
        system_time
    );
    assert_eq!(millis, to_timestamp(&system_time)?);

    // negative timestamp is an error
    assert!(to_system_time(-1).is_err());

    Ok(())
}

#[test]
fn list_offset() -> Result<()> {
    const EARLIEST: i64 = -2;
    const LATEST: i64 = -1;

    // Earliest round-trips
    assert_eq!(EARLIEST, i64::try_from(ListOffset::try_from(EARLIEST)?)?);

    // Latest round-trips
    assert_eq!(LATEST, i64::try_from(ListOffset::try_from(LATEST)?)?);

    // Timestamp round-trips
    let millis: i64 = 1_700_000_000_000;
    let offset = ListOffset::try_from(millis)?;
    assert_eq!(
        ListOffset::Timestamp(SystemTime::UNIX_EPOCH + Duration::from_millis(millis as u64)),
        offset
    );
    assert_eq!(millis, i64::try_from(offset)?);

    // MAX_TIMESTAMP (-3), EARLIEST_LOCAL_TIMESTAMP (-4) and LATEST_TIERED_TIMESTAMP (-5) are
    // named sentinels nisshi does not implement, and are rejected with a dedicated error
    // rather than an incidental one.
    for sentinel in [-3i64, -4, -5] {
        assert!(matches!(
            ListOffset::try_from(sentinel),
            Err(Error::UnsupportedListOffsetTimestamp(timestamp)) if timestamp == sentinel
        ));
    }

    Ok(())
}

#[test]
fn config_type() {
    const BOOLEAN: i8 = 1;
    const STRING: i8 = 2;
    const INT: i8 = 3;
    const SHORT: i8 = 4;
    const LONG: i8 = 5;
    const DOUBLE: i8 = 6;
    const LIST: i8 = 7;
    const CLASS: i8 = 8;
    const PASSWORD: i8 = 9;

    assert_eq!(BOOLEAN, i8::from(ConfigType::from(BOOLEAN)));
    assert_eq!(STRING, i8::from(ConfigType::from(STRING)));
    assert_eq!(INT, i8::from(ConfigType::from(INT)));
    assert_eq!(SHORT, i8::from(ConfigType::from(SHORT)));
    assert_eq!(LONG, i8::from(ConfigType::from(LONG)));
    assert_eq!(DOUBLE, i8::from(ConfigType::from(DOUBLE)));
    assert_eq!(LIST, i8::from(ConfigType::from(LIST)));
    assert_eq!(CLASS, i8::from(ConfigType::from(CLASS)));
    assert_eq!(PASSWORD, i8::from(ConfigType::from(PASSWORD)));

    // unrecognized values map to Unknown
    assert_eq!(0i8, i8::from(ConfigType::from(99)));
    assert_eq!(0i8, i8::from(ConfigType::from(0)));
}

#[test]
fn allocation_size() -> Result<()> {
    let mut q: Option<String> = Some("hey".into());

    assert_eq!(size_of::<u16>() + 3, q.maximum_allocation_size()?);

    q = None;
    assert_eq!(size_of::<u32>(), q.maximum_allocation_size()?);

    Ok(())
}

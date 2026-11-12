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

//! A topic's `message.timestamp.type`: the clock of each record's timestamp. `CreateTime` keeps the
//! producer's timestamp. `LogAppendTime` replaces it with the broker's time when the broker stores
//! the batch.

use nisshi_sans_io::{ErrorCode, TimestampType, describe_configs_response::DescribeConfigsResult};
use tracing::debug;

use crate::{Error, Result};

/// The topic configuration key.
pub const KEY: &str = "message.timestamp.type";

/// Parses `configured` as `CreateTime` or `LogAppendTime`, and ignores surrounding whitespace. The
/// match is case-sensitive. Kafka parses a `message.timestamp.type` value the same way
/// ([`LogConfig.java#L312-L313`], [`ConfigDef.java#L1044-L1049`]).
///
/// The error is the message that Kafka sends with `INVALID_CONFIG`.
///
/// [`LogConfig.java#L312-L313`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L312-L313
/// [`ConfigDef.java#L1044-L1049`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/config/ConfigDef.java#L1044-L1049
pub fn parse(configured: &str) -> Result<TimestampType, String> {
    match configured.trim() {
        "CreateTime" => Ok(TimestampType::CreateTime),
        "LogAppendTime" => Ok(TimestampType::LogAppendTime),
        trimmed => Err(format!(
            "Invalid value {trimmed} for configuration {KEY}: String must be one of: CreateTime, \
             LogAppendTime"
        )),
    }
}

/// Returns the topic's timestamp type from `configured`, its stored value. The type is
/// `CreateTime`, Kafka's default, when the topic does not set the key.
///
/// The type is also `CreateTime` when [`parse`] rejects the stored value. This function uses the
/// default instead of rejecting every produce, because a mistyped value would then stop all writes
/// to the topic.
pub fn timestamp_type(topic: &str, configured: Option<&str>) -> TimestampType {
    let Some(configured) = configured else {
        return TimestampType::CreateTime;
    };

    parse(configured).unwrap_or_else(|reason| {
        debug!(topic, reason, "using the default message.timestamp.type");
        TimestampType::CreateTime
    })
}

/// Returns the timestamp type of `topic` from `described`, its `DescribeConfigs` result for
/// [`KEY`].
///
/// Returns `CreateTime` for a topic that does not exist, because the caller's write then answers
/// with the error code for an unknown topic. Returns [`Error::Api`] for a result with any other
/// error code. That result does not carry the topic's configs, so `CreateTime` would store the
/// records of a `LogAppendTime` topic with the producer's timestamps.
pub fn described(topic: &str, described: DescribeConfigsResult) -> Result<TimestampType> {
    match ErrorCode::try_from(described.error_code)? {
        ErrorCode::None | ErrorCode::UnknownTopicOrPartition => {}
        error_code => return Err(Error::Api(error_code)),
    }

    let configured = described
        .configs
        .unwrap_or_default()
        .into_iter()
        .find(|config| config.name == KEY)
        .and_then(|config| config.value);

    Ok(timestamp_type(topic, configured.as_deref()))
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use nisshi_sans_io::describe_configs_response::DescribeConfigsResourceResult;

    use super::*;

    const TOPIC: &str = "abc";

    #[test]
    fn an_unset_type_is_create_time() {
        assert_eq!(TimestampType::CreateTime, timestamp_type(TOPIC, None));
    }

    #[test]
    fn a_set_type_is_used() {
        assert_eq!(
            TimestampType::LogAppendTime,
            timestamp_type(TOPIC, Some("LogAppendTime"))
        );
        assert_eq!(
            TimestampType::CreateTime,
            timestamp_type(TOPIC, Some("CreateTime"))
        );
    }

    #[test]
    fn surrounding_whitespace_is_ignored() {
        assert_eq!(Ok(TimestampType::LogAppendTime), parse(" LogAppendTime "));
    }

    #[test]
    fn another_case_is_rejected_with_kafkas_message() {
        assert_eq!(
            Err(String::from(
                "Invalid value logappendtime for configuration message.timestamp.type: String \
                 must be one of: CreateTime, LogAppendTime"
            )),
            parse("logappendtime")
        );
    }

    #[test]
    fn an_invalid_stored_type_is_create_time() {
        assert_eq!(
            TimestampType::CreateTime,
            timestamp_type(TOPIC, Some("Bogus"))
        );
    }

    fn described_with(error_code: ErrorCode, value: Option<&str>) -> DescribeConfigsResult {
        DescribeConfigsResult::default()
            .error_code(error_code.into())
            .configs(Some(
                value
                    .map(|value| {
                        DescribeConfigsResourceResult::default()
                            .name(KEY.into())
                            .value(Some(value.into()))
                    })
                    .into_iter()
                    .collect(),
            ))
    }

    #[test]
    fn a_described_type_is_used() -> Result<()> {
        assert_eq!(
            TimestampType::LogAppendTime,
            described(
                TOPIC,
                described_with(ErrorCode::None, Some("LogAppendTime"))
            )?
        );
        Ok(())
    }

    #[test]
    fn an_unknown_topic_is_create_time() -> Result<()> {
        assert_eq!(
            TimestampType::CreateTime,
            described(
                TOPIC,
                described_with(ErrorCode::UnknownTopicOrPartition, None)
            )?
        );
        Ok(())
    }

    #[test]
    fn a_failed_describe_is_an_error() {
        assert_matches!(
            described(TOPIC, described_with(ErrorCode::UnknownServerError, None)),
            Err(Error::Api(ErrorCode::UnknownServerError))
        );
    }
}

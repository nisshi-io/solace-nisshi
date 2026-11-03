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

//! Topic configuration rules from Kafka: the values that Kafka accepts for a topic configuration,
//! and the list-typed configuration that the `APPEND` and `SUBTRACT` operations of
//! [`IncrementalAlterConfigsRequest`](nisshi_sans_io::IncrementalAlterConfigsRequest) alter.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use nisshi_sans_io::{ConfigResource, ErrorCode, OpType};

use crate::{Error, Result, message_timestamp_type};

/// Checks `value` against Kafka's rules for the topic configuration `key`. Returns the value to
/// store, or Kafka's [`ErrorCode::InvalidConfig`] message when Kafka rejects it.
///
/// This function checks `retention.ms` and `retention.bytes`, with the rules that Kafka defines for
/// them in [`LogConfig`], and `message.timestamp.type` with [`message_timestamp_type::parse`]. It
/// returns the value of any other configuration without a check. [`crate::max_message_bytes`]
/// checks `max.message.bytes`. The storage engines read the retention values in their maintenance
/// queries, so a value that doesn't parse can stop maintenance for every topic.
///
/// For a retention configuration, it returns the plain number that it parses, without white space
/// or a `+`, because the maintenance queries read anything else as no limit. For
/// `message.timestamp.type`, it returns the value without surrounding white space.
///
/// [`LogConfig`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L277-L279
pub fn validate_topic_config(key: &str, value: Option<&str>) -> Result<Option<String>, String> {
    if key == message_timestamp_type::KEY {
        return value
            .map(|value| message_timestamp_type::parse(value).map(|_| value.trim().to_owned()))
            .transpose();
    }

    let minimum: Option<i64> = match key {
        "retention.bytes" => None,
        "retention.ms" => Some(-1),
        _ => return Ok(value.map(ToOwned::to_owned)),
    };
    let Some(value) = value else {
        return Ok(None);
    };

    // Kafka trims the value before it parses it, in `ConfigDef.parseType`.
    let reason = match value.trim().parse::<i64>() {
        Ok(number) => match minimum {
            Some(minimum) if number < minimum => format!("Value must be at least {minimum}"),
            _ => return Ok(Some(number.to_string())),
        },
        Err(_) => "Not a number of type LONG".to_owned(),
    };

    Err(format!(
        "Invalid value {value} for configuration {key}: {reason}"
    ))
}

/// Kafka's default `cleanup.policy`, which applies to a topic that doesn't set one.
pub(crate) const DEFAULT_CLEANUP_POLICY: &str = "delete";

/// Kafka's default `retention.ms`, 7 days, which the storage engines that enforce retention apply
/// to a topic that doesn't set one.
pub const DEFAULT_RETENTION: Duration = Duration::from_hours(7 * 24);

/// The topic configuration keys whose type is `LIST`.
///
/// Kafka defines them in `LogConfig`: [`cleanup.policy`],
/// [`leader.replication.throttled.replicas` and `follower.replication.throttled.replicas`].
///
/// [`cleanup.policy`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L295
/// [`leader.replication.throttled.replicas` and `follower.replication.throttled.replicas`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L320-L323
pub const TOPIC_LIST_CONFIGS: [&str; 3] = [
    "cleanup.policy",
    "leader.replication.throttled.replicas",
    "follower.replication.throttled.replicas",
];

/// Whether `key` is a `LIST` configuration of `resource`, the only kind that `APPEND` and
/// `SUBTRACT` may alter.
pub fn is_list_config(resource: ConfigResource, key: &str) -> bool {
    resource == ConfigResource::Topic && TOPIC_LIST_CONFIGS.contains(&key)
}

/// Returns the value of the configuration `key` of `resource` after applying `op` with `value` to
/// `current`, or `None` when `op` removes the key.
///
/// `APPEND` and `SUBTRACT` follow Kafka's [`incrementalAlterConfigResource`]
/// and [`getParts`]:
///
/// - `APPEND` adds each item of `value` that isn't already present;
/// - `SUBTRACT` removes the first occurrence of each item of `value`.
///
/// An unset `current` is the default of `key`, as in Kafka, so `APPEND compact` to an unset
/// `cleanup.policy` gives `delete,compact`.
///
/// # Errors
///
/// Returns [`ErrorCode::InvalidConfig`] for `APPEND` or `SUBTRACT` on a key that
/// [isn't a list](is_list_config).
///
/// [`incrementalAlterConfigResource`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java#L232-L250
/// [`getParts`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java#L389-L404
pub fn apply_op(
    resource: ConfigResource,
    key: &str,
    current: Option<&str>,
    op: OpType,
    value: Option<&str>,
) -> Result<Option<String>> {
    let append = match op {
        OpType::Set => return Ok(value.map(ToOwned::to_owned)),
        OpType::Delete => return Ok(None),
        OpType::Append => true,
        OpType::Subtract => false,
    };

    if !is_list_config(resource, key) {
        return Err(Error::Api(ErrorCode::InvalidConfig));
    }

    let default = if key == "cleanup.policy" {
        DEFAULT_CLEANUP_POLICY
    } else {
        ""
    };

    let mut items: Vec<&str> = current
        .unwrap_or(default)
        .split(',')
        .filter(|item| !item.is_empty())
        .collect();

    // Kafka keeps an empty item of `value` (from `a,,b`) and then rejects the result when it
    // validates the new value. We ignore an empty item instead, because Nisshi doesn't validate
    // list values and would store it.
    let changes = value
        .unwrap_or_default()
        .split(',')
        .filter(|item| !item.is_empty());

    // The sets keep the cost linear in the number of items: a client sets the size of `value`, and
    // the backends apply it inside a write transaction.
    if append {
        let mut present: HashSet<&str> = items.iter().copied().collect();

        for item in changes {
            if present.insert(item) {
                items.push(item);
            }
        }
    } else {
        let mut removals: HashMap<&str, usize> = HashMap::new();

        for item in changes {
            *removals.entry(item).or_default() += 1;
        }

        items.retain(|item| match removals.get_mut(item) {
            Some(remaining) if *remaining > 0 => {
                *remaining -= 1;
                false
            }
            _ => true,
        });
    }

    Ok(Some(items.join(",")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLEANUP_POLICY: &str = "cleanup.policy";

    fn apply(current: Option<&str>, op: OpType, value: &str) -> Result<Option<String>> {
        apply_op(
            ConfigResource::Topic,
            CLEANUP_POLICY,
            current,
            op,
            Some(value),
        )
    }

    #[test]
    fn list_configs_are_topic_only() {
        assert!(is_list_config(ConfigResource::Topic, CLEANUP_POLICY));
        assert!(is_list_config(
            ConfigResource::Topic,
            "follower.replication.throttled.replicas"
        ));
        assert!(!is_list_config(ConfigResource::Topic, "retention.ms"));
        assert!(!is_list_config(ConfigResource::Broker, CLEANUP_POLICY));
    }

    #[test]
    fn set_and_delete() -> Result<()> {
        assert_eq!(
            Some("compact"),
            apply(Some("delete"), OpType::Set, "compact")?.as_deref()
        );
        assert_eq!(None, apply(Some("delete"), OpType::Delete, "compact")?);
        Ok(())
    }

    #[test]
    fn set_and_delete_any_key() -> Result<()> {
        assert_eq!(
            Some("1000"),
            apply_op(
                ConfigResource::Topic,
                "retention.ms",
                None,
                OpType::Set,
                Some("1000")
            )?
            .as_deref()
        );
        Ok(())
    }

    #[test]
    fn append_or_subtract_non_list_is_invalid_config() {
        for op in [OpType::Append, OpType::Subtract] {
            assert!(matches!(
                apply_op(
                    ConfigResource::Topic,
                    "retention.ms",
                    None,
                    op,
                    Some("1000")
                ),
                Err(Error::Api(ErrorCode::InvalidConfig))
            ));
        }
    }

    /// A topic without a `cleanup.policy` deletes old records, so `APPEND compact` must keep
    /// `delete`, or the topic stops deleting old records.
    #[test]
    fn append_to_unset_cleanup_policy_keeps_kafka_default() -> Result<()> {
        assert_eq!(
            Some("delete,compact"),
            apply(None, OpType::Append, "compact")?.as_deref()
        );
        Ok(())
    }

    /// Kafka's default for a replication throttle list is an empty list, so the `delete` default
    /// of `cleanup.policy` must not end up in it.
    #[test]
    fn append_to_unset_replication_throttle_list_starts_empty() -> Result<()> {
        assert_eq!(
            Some("0:111"),
            apply_op(
                ConfigResource::Topic,
                "leader.replication.throttled.replicas",
                None,
                OpType::Append,
                Some("0:111")
            )?
            .as_deref()
        );
        Ok(())
    }

    #[test]
    fn retention_ms_accepts_minus_1_and_above() {
        for value in ["-1", "0", "2592000000"] {
            assert_eq!(
                Ok(Some(value.to_owned())),
                validate_topic_config("retention.ms", Some(value))
            );
        }
    }

    /// The maintenance queries read only a plain number, so a value that Kafka accepts with white
    /// space or a `+` around it must be stored as that plain number. Stored as sent, the queries
    /// read it as no limit, and the topic keeps every record.
    #[test]
    fn retention_ms_is_stored_as_the_number_that_kafka_parses() {
        for value in [" 600000 ", "+600000", "600000\n", "\t600000"] {
            assert_eq!(
                Ok(Some("600000".to_owned())),
                validate_topic_config("retention.ms", Some(value))
            );
        }
    }

    /// The produce path reads the stored `message.timestamp.type`, so a value that Kafka accepts
    /// with white space around it is stored without that white space.
    #[test]
    fn message_timestamp_type_is_stored_without_surrounding_white_space() {
        assert_eq!(
            Ok(Some("LogAppendTime".to_owned())),
            validate_topic_config("message.timestamp.type", Some(" LogAppendTime\n"))
        );
    }

    #[test]
    fn message_timestamp_type_rejects_a_value_that_kafka_rejects() {
        assert_eq!(
            Err(String::from(
                "Invalid value logappendtime for configuration message.timestamp.type: String \
                 must be one of: CreateTime, LogAppendTime"
            )),
            validate_topic_config("message.timestamp.type", Some("logappendtime"))
        );
    }

    #[test]
    fn retention_ms_rejects_values_below_minus_1() {
        assert_eq!(
            Err(
                "Invalid value -2 for configuration retention.ms: Value must be at least -1"
                    .to_owned()
            ),
            validate_topic_config("retention.ms", Some("-2"))
        );
    }

    #[test]
    fn retention_ms_rejects_a_value_that_is_not_a_long() {
        for value in ["abc", "", "1.5", "9223372036854775808"] {
            assert_eq!(
                Err(format!(
                    "Invalid value {value} for configuration retention.ms: Not a number of type LONG"
                )),
                validate_topic_config("retention.ms", Some(value))
            );
        }
    }

    /// Kafka sets no minimum for `retention.bytes`, and reads any negative value as no limit.
    #[test]
    fn retention_bytes_accepts_any_long() {
        for value in ["-2", "-1", "0", "1073741824", "9223372036854775807"] {
            assert_eq!(
                Ok(Some(value.to_owned())),
                validate_topic_config("retention.bytes", Some(value))
            );
        }
    }

    /// A `retention.bytes` that doesn't fit in a long fails the PostgreSQL maintenance statement,
    /// which stops maintenance for every topic, so the broker must reject it as Kafka does.
    #[test]
    fn retention_bytes_rejects_a_value_that_is_not_a_long() {
        for value in ["abc", "1.5", "99999999999999999999"] {
            assert_eq!(
                Err(format!(
                    "Invalid value {value} for configuration retention.bytes: Not a number of type LONG"
                )),
                validate_topic_config("retention.bytes", Some(value))
            );
        }
    }

    #[test]
    fn other_configs_are_stored_as_sent() {
        assert_eq!(
            Ok(Some(" compact ".to_owned())),
            validate_topic_config("cleanup.policy", Some(" compact "))
        );
    }

    #[test]
    fn append_present_item_is_unchanged() -> Result<()> {
        assert_eq!(
            Some("delete,compact"),
            apply(Some("delete,compact"), OpType::Append, "compact")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn append_multiple_items() -> Result<()> {
        assert_eq!(
            Some("x,a,b"),
            apply(Some("x"), OpType::Append, "a,b,a")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn append_keeps_existing_duplicates() -> Result<()> {
        assert_eq!(
            Some("a,a,b"),
            apply(Some("a,a"), OpType::Append, "a,b")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_to_empty() -> Result<()> {
        assert_eq!(
            Some(""),
            apply(Some("delete"), OpType::Subtract, "delete")?.as_deref()
        );
        assert_eq!(
            Some(""),
            apply(None, OpType::Subtract, "delete")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_multiple_items() -> Result<()> {
        assert_eq!(
            Some("b"),
            apply(Some("a,b,c"), OpType::Subtract, "a,c")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_removes_first_occurrence() -> Result<()> {
        assert_eq!(
            Some("b,a"),
            apply(Some("a,b,a"), OpType::Subtract, "a")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_repeated_item_removes_as_many_occurrences() -> Result<()> {
        assert_eq!(
            Some("b,a"),
            apply(Some("a,b,a,a"), OpType::Subtract, "a,a")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_absent_item_is_unchanged() -> Result<()> {
        assert_eq!(
            Some("compact"),
            apply(Some("compact"), OpType::Subtract, "delete")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn empty_items_are_ignored() -> Result<()> {
        assert_eq!(
            Some("a,b"),
            apply(Some(""), OpType::Append, "a,,b,")?.as_deref()
        );

        assert_eq!(
            Some("delete"),
            apply(Some("delete,,"), OpType::Subtract, ",")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn large_append_and_subtract() -> Result<()> {
        let value = (0..100_000)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let appended = apply(Some(""), OpType::Append, &value)?;
        assert_eq!(Some(value.as_str()), appended.as_deref());

        assert_eq!(
            Some(""),
            apply(appended.as_deref(), OpType::Subtract, &value)?.as_deref()
        );
        Ok(())
    }
}

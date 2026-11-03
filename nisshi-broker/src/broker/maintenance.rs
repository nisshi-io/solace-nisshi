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

//! The broker's sweep interval options, read from the storage URL's query string.

use crate::{Error, Result};
use std::{str::FromStr, time::Duration};
use url::{Url, form_urlencoded};

pub(crate) const MAINTENANCE_INTERVAL: &str = "maintenance_interval";
pub(crate) const TRANSACTION_MAINTENANCE_INTERVAL: &str = "transaction_maintenance_interval";

/// The longest sweep interval that the broker accepts.
///
/// A late tick makes `tokio::time::Interval` add the period to `Instant::now()`
/// without an overflow check, so a very large period panics the broker. One year is
/// far above any useful sweep interval and far below that overflow.
const MAXIMUM_INTERVAL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The sweep intervals that the broker reads from a storage URL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Intervals {
    pub(crate) maintenance: Option<Duration>,
    pub(crate) transaction_maintenance: Option<Duration>,
}

/// Removes the sweep interval options from `storage`, and returns them parsed.
///
/// The broker owns these options, and no storage engine reads them, so the URL that
/// this returns is the one that the broker gives to the storage engine.
///
/// # Errors
///
/// [`Error::InvalidStorageOptionValue`] when an interval option is present and
/// [`parse_interval`] rejects its value.
pub(crate) fn take_intervals(mut storage: Url) -> Result<(Url, Intervals)> {
    let mut intervals = Intervals::default();
    let mut retained = Vec::new();

    // The other options are kept as written. Serializing them again would form-encode them, and a
    // space (`%20`) would become `+`, which a Postgres `options` value reads literally.
    for segment in storage.query().unwrap_or_default().split('&') {
        match form_urlencoded::parse(segment.as_bytes()).next() {
            Some((key, value)) if key == MAINTENANCE_INTERVAL => {
                intervals.maintenance = Some(parse_interval(&key, &value)?);
            }
            Some((key, value)) if key == TRANSACTION_MAINTENANCE_INTERVAL => {
                intervals.transaction_maintenance = Some(parse_interval(&key, &value)?);
            }
            _ => retained.push(segment),
        }
    }

    if intervals == Intervals::default() {
        return Ok((storage, intervals));
    }

    if retained.is_empty() {
        storage.set_query(None);
    } else {
        storage.set_query(Some(&retained.join("&")));
    }

    Ok((storage, intervals))
}

/// Parses the value of the sweep interval option `option`.
///
/// The value takes a `human_units` duration (`90s`, `10m`), or a `humantime` duration
/// (`1h30m`, `5min`). A value must name its unit: a bare number is rejected instead
/// of read as seconds, because a number of milliseconds then becomes a period of days.
///
/// # Errors
///
/// [`Error::InvalidStorageOptionValue`] when the value is a bare number, does not
/// parse, is zero (`tokio::time::interval` panics on a zero period), or is longer
/// than one year.
pub(crate) fn parse_interval(option: &str, value: &str) -> Result<Duration> {
    let invalid = || Error::InvalidStorageOptionValue {
        option: option.to_owned(),
        value: value.to_owned(),
    };

    if value.trim().chars().all(|c| c.is_ascii_digit()) {
        return Err(invalid());
    }

    human_units::Duration::from_str(value)
        .map(|duration| duration.0)
        .or_else(|_| value.parse::<humantime::Duration>().map(Into::into))
        .ok()
        .filter(|duration| !duration.is_zero() && *duration <= MAXIMUM_INTERVAL)
        .ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invalid(error: Error, expected_option: &str, expected_value: &str) -> bool {
        matches!(
            error,
            Error::InvalidStorageOptionValue { option, value }
                if option == expected_option && value == expected_value
        )
    }

    #[test]
    fn parses_human_units_and_humantime_durations() -> Result<()> {
        for (value, expected) in [
            ("90s", Duration::from_secs(90)),
            ("10m", Duration::from_mins(10)),
            ("500ms", Duration::from_millis(500)),
            ("1h30m", Duration::from_mins(90)),
            ("5min", Duration::from_mins(5)),
            ("365d", MAXIMUM_INTERVAL),
        ] {
            assert_eq!(
                expected,
                parse_interval(MAINTENANCE_INTERVAL, value)?,
                "{value}"
            );
        }

        Ok(())
    }

    #[test]
    fn rejects_bare_zero_unparsable_and_too_long_values() {
        for value in [
            "",
            "600000",
            "0s",
            "0m",
            "ten minutes",
            "10x",
            "366d",
            "10000000000000000000s",
        ] {
            let error = parse_interval(MAINTENANCE_INTERVAL, value).unwrap_err();
            assert!(invalid(error, MAINTENANCE_INTERVAL, value), "{value}");
        }
    }

    #[test]
    fn take_intervals_without_options_leaves_url_unchanged() -> Result<()> {
        let storage = Url::parse("sqlite://nisshi.db?vacuum_into=/tmp/x")?;

        let (retained, intervals) = take_intervals(storage.clone())?;

        assert_eq!(storage, retained);
        assert_eq!(Intervals::default(), intervals);

        Ok(())
    }

    #[test]
    fn take_intervals_removes_interval_options_and_keeps_the_rest() -> Result<()> {
        let (retained, intervals) = take_intervals(Url::parse(
            "sqlite://nisshi.db?maintenance_interval=1m&vacuum_into=/tmp/x&transaction_maintenance_interval=5s",
        )?)?;

        assert_eq!("sqlite://nisshi.db?vacuum_into=/tmp/x", retained.as_str());
        assert_eq!(
            Intervals {
                maintenance: Some(Duration::from_mins(1)),
                transaction_maintenance: Some(Duration::from_secs(5)),
            },
            intervals
        );

        let (retained, _) = take_intervals(Url::parse("memory://?maintenance_interval=1m")?)?;
        assert_eq!(None, retained.query());

        Ok(())
    }

    #[test]
    fn take_intervals_rejects_an_invalid_interval() -> Result<()> {
        let error = take_intervals(Url::parse("memory://?transaction_maintenance_interval=0s")?)
            .unwrap_err();

        assert!(invalid(error, TRANSACTION_MAINTENANCE_INTERVAL, "0s"));

        Ok(())
    }

    /// A Postgres `options` value keeps its `%20`, whatever else is removed beside it.
    #[test]
    fn take_intervals_keeps_the_other_options_byte_for_byte() -> Result<()> {
        let (retained, intervals) = take_intervals(Url::parse(
            "postgres://user:pw@host/db?maintenance_interval=60s&options=-c%20statement_timeout%3D5000&a=b+c&empty=&transaction_maintenance_interval=5s&x=%2F",
        )?)?;

        assert_eq!(
            "postgres://user:pw@host/db?options=-c%20statement_timeout%3D5000&a=b+c&empty=&x=%2F",
            retained.as_str()
        );
        assert_eq!(Some(Duration::from_mins(1)), intervals.maintenance);
        assert_eq!(
            Some(Duration::from_secs(5)),
            intervals.transaction_maintenance
        );

        Ok(())
    }
}

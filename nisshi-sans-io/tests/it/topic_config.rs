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

//! Checks that each `tansu.*` topic configuration key behaves like its `nisshi.*` key, and that
//! using the alias logs one deprecation warning.

use std::{
    assert_matches, io,
    sync::{Arc, Mutex},
};

use nisshi_sans_io::topic_config::{self, AliasWarnings};

const DEPRECATION: &str = "deprecated topic configuration key";

const KEYS: [(&str, &str); 10] = [
    ("tansu.batch", topic_config::BATCH),
    ("tansu.batch.timeout_ms", topic_config::BATCH_TIMEOUT_MS),
    ("tansu.batch.max_records", topic_config::BATCH_MAX_RECORDS),
    ("tansu.lake.sink", topic_config::LAKE_SINK),
    ("tansu.lake.partition", topic_config::LAKE_PARTITION),
    ("tansu.lake.z_order", topic_config::LAKE_Z_ORDER),
    ("tansu.lake.normalize", topic_config::LAKE_NORMALIZE),
    (
        "tansu.lake.normalize.separator",
        topic_config::LAKE_NORMALIZE_SEPARATOR,
    ),
    ("tansu.schema.validation", topic_config::SCHEMA_VALIDATION),
    ("tansu.virtual", topic_config::VIRTUAL),
];

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Returns the log text that `run` writes.
fn log_of(run: impl FnOnce()) -> String {
    let capture = Capture::default();
    let writer = capture.clone();

    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, run);

    let written = capture
        .0
        .lock()
        .map(|buffer| buffer.clone())
        .unwrap_or_default();
    String::from_utf8_lossy(&written).into_owned()
}

#[test]
fn alias_supplies_the_value_of_its_key() {
    let warnings = AliasWarnings::default();

    for (alias, key) in KEYS {
        assert_eq!(
            Some("value"),
            warnings.lookup([(alias, "value")], key),
            "{alias} should supply {key}"
        );
        assert_eq!(
            Some("value"),
            warnings.lookup([(key, "value")], key),
            "{key} should supply itself"
        );
    }
}

#[test]
fn lookup_by_follows_the_alias_and_prefers_the_key() {
    let warnings = AliasWarnings::default();

    for (alias, key) in KEYS {
        assert_eq!(
            Some("old"),
            warnings.lookup_by(key, |name| (name == alias).then_some("old")),
            "{alias}"
        );
    }

    let both = |name: &str| match name {
        "tansu.virtual" => Some("old"),
        "nisshi.virtual" => Some("new"),
        _ => None,
    };
    assert_eq!(Some("new"), warnings.lookup_by(topic_config::VIRTUAL, both));
    assert_eq!(None, warnings.lookup_by(topic_config::BATCH, both));
}

#[test]
fn alias_of_another_key_does_not_supply_the_key() {
    let warnings = AliasWarnings::default();

    assert_eq!(
        None,
        warnings.lookup([("tansu.batch.timeout_ms", "1")], topic_config::BATCH)
    );
    assert_eq!(
        None,
        warnings.lookup([("tansu.unknown", "1")], "nisshi.unknown")
    );
}

#[test]
fn key_wins_over_its_alias_in_either_order() {
    let warnings = AliasWarnings::default();

    assert_eq!(
        Some("new"),
        warnings.lookup(
            [("tansu.lake.sink", "old"), ("nisshi.lake.sink", "new")],
            topic_config::LAKE_SINK
        )
    );
    assert_eq!(
        Some("new"),
        warnings.lookup(
            [("nisshi.lake.sink", "new"), ("tansu.lake.sink", "old")],
            topic_config::LAKE_SINK
        )
    );
}

#[test]
fn alias_logs_one_deprecation_warning_however_often_it_is_used() {
    let warnings = AliasWarnings::default();

    let log = log_of(|| {
        for _ in 0..5 {
            _ = warnings.lookup([("tansu.virtual", "true")], topic_config::VIRTUAL);
        }
    });

    assert_eq!(1, log.matches(DEPRECATION).count(), "{log}");
    assert!(log.contains("tansu.virtual"), "{log}");
    assert!(log.contains("nisshi.virtual"), "{log}");
}

#[test]
fn each_alias_logs_its_own_warning() {
    let warnings = AliasWarnings::default();

    let log = log_of(|| {
        _ = warnings.lookup([("tansu.batch", "true")], topic_config::BATCH);
        _ = warnings.lookup([("tansu.virtual", "true")], topic_config::VIRTUAL);
    });

    assert_eq!(2, log.matches(DEPRECATION).count(), "{log}");
}

#[test]
fn key_logs_no_warning() {
    let warnings = AliasWarnings::default();

    let log = log_of(|| {
        _ = warnings.lookup([("nisshi.virtual", "true")], topic_config::VIRTUAL);
        _ = warnings.resolve([("nisshi.virtual", "true"), ("tansu.unknown", "x")]);
    });

    assert_eq!("", log);
}

#[test]
fn warning_names_the_key_and_never_the_value() {
    let warnings = AliasWarnings::default();

    let log = log_of(|| {
        _ = warnings.lookup(
            [("tansu.schema.validation", "secret-value")],
            topic_config::SCHEMA_VALIDATION,
        );
    });

    assert!(log.contains("tansu.schema.validation"), "{log}");
    assert!(!log.contains("secret-value"), "{log}");
}

#[test]
fn resolve_renames_each_alias_to_its_key() {
    let warnings = AliasWarnings::default();

    for (alias, key) in KEYS {
        let resolved = warnings.resolve([(alias, "value")]);

        assert_matches!(resolved.as_slice(), [(name, "value")] if name == key);
    }
}

#[test]
fn resolve_renames_the_alias_of_a_generated_column_and_logs_the_family_once() {
    let warnings = AliasWarnings::default();

    let log = log_of(|| {
        let resolved = warnings.resolve([
            ("tansu.lake.generate.date", "CAST(a AS DATE)"),
            ("tansu.lake.generate.year", "CAST(b AS INT)"),
        ]);

        assert_matches!(
            resolved.as_slice(),
            [(date, _), (year, _)]
                if date == "nisshi.lake.generate.date" && year == "nisshi.lake.generate.year"
        );
    });

    assert_eq!(1, log.matches(DEPRECATION).count(), "{log}");
    assert!(log.contains("tansu.lake.generate.*"), "{log}");
    assert!(
        !log.contains("generate.date"),
        "a column name is client input: {log}"
    );
}

#[test]
fn resolve_drops_the_alias_that_its_key_overrides() {
    let warnings = AliasWarnings::default();

    let resolved = warnings.resolve([
        ("tansu.lake.sink", "false"),
        ("nisshi.lake.sink", "true"),
        ("tansu.lake.normalize", "true"),
    ]);

    assert_matches!(
        resolved.as_slice(),
        [(sink, "true"), (normalize, "true")]
            if sink == "nisshi.lake.sink" && normalize == "nisshi.lake.normalize"
    );
}

#[test]
fn resolve_keeps_a_name_that_is_not_an_alias() {
    let warnings = AliasWarnings::default();

    let resolved = warnings.resolve([("tansu.unknown", "1"), ("retention.ms", "2")]);

    assert_matches!(
        resolved.as_slice(),
        [(unknown, "1"), (retention, "2")]
            if unknown == "tansu.unknown" && retention == "retention.ms"
    );
}

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

//! The field shorthand of a topic's Avro schema: a record field that names its type as a string
//! and carries that type's attributes beside its own, such as
//! `{"name": "value", "type": "array", "items": "int"}` or
//! `{"name": "timestamp", "type": "long", "logicalType": "timestamp-micros"}`.
//!
//! The [Avro specification] gives a field only the attributes `name`, `doc`, `type`, `order`,
//! `aliases` and `default`, and a parser that follows it rejects an inline complex type and
//! ignores a logical type on a field. Nisshi accepts the shorthand in a topic's schema, and
//! [`expand`] rewrites it to the form in the specification before the schema is parsed.
//!
//! [Avro specification]: https://avro.apache.org/docs/1.12.0/specification/#schema-record

use serde_json::{Map, Value as JsonValue};

/// The attributes that the Avro specification gives a record field. Each other attribute of a
/// field in the shorthand belongs to the field's type.
const FIELD_ATTRIBUTES: [&str; 6] = ["name", "doc", "type", "order", "aliases", "default"];

/// The complex types that the Avro specification requires to have a name.
const NAMED_TYPES: [&str; 3] = ["enum", "fixed", "record"];

/// Returns `schema` with the shorthand of each record field, at any depth, expanded: the
/// attributes of the field's type move into a nested `type` object.
///
/// A named type that a field declares inline takes the field's name, because the shorthand
/// has one `name` attribute for both.
pub(crate) fn expand(schema: JsonValue) -> JsonValue {
    match schema {
        JsonValue::Array(union) => JsonValue::Array(union.into_iter().map(expand).collect()),
        JsonValue::Object(object) => JsonValue::Object(expand_object(object)),
        primitive => primitive,
    }
}

fn expand_object(mut object: Map<String, JsonValue>) -> Map<String, JsonValue> {
    for key in ["type", "items", "values"] {
        if let Some(inner) = object.remove(key) {
            _ = object.insert(key.to_owned(), expand(inner));
        }
    }

    if let Some(JsonValue::Array(fields)) = object.remove("fields") {
        _ = object.insert(
            "fields".to_owned(),
            JsonValue::Array(fields.into_iter().map(expand_field).collect()),
        );
    }

    object
}

fn expand_field(field: JsonValue) -> JsonValue {
    let JsonValue::Object(field) = field else {
        return field;
    };

    let names_its_type_as_string = field.get("type").is_some_and(JsonValue::is_string);
    let has_type_attributes = field
        .keys()
        .any(|key| !FIELD_ATTRIBUTES.contains(&key.as_str()));

    if !(names_its_type_as_string && has_type_attributes) {
        return JsonValue::Object(expand_object(field));
    }

    let (mut field_attributes, mut type_attributes): (Map<_, _>, Map<_, _>) = field
        .into_iter()
        .partition(|(key, _)| FIELD_ATTRIBUTES.contains(&key.as_str()));

    if let Some(type_name) = field_attributes.remove("type") {
        let is_named = type_name
            .as_str()
            .is_some_and(|type_name| NAMED_TYPES.contains(&type_name));

        if is_named && let Some(name) = field_attributes.get("name") {
            _ = type_attributes.insert("name".to_owned(), name.clone());
        }

        _ = type_attributes.insert("type".to_owned(), type_name);
    }

    _ = field_attributes.insert(
        "type".to_owned(),
        JsonValue::Object(expand_object(type_attributes)),
    );

    JsonValue::Object(field_attributes)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn field_with_primitive_type_is_unchanged() {
        let schema = json!({
            "type": "record",
            "name": "test",
            "fields": [{"name": "key", "type": "int", "doc": "the key", "default": 0}]
        });

        assert_eq!(schema.clone(), expand(schema));
    }

    #[test]
    fn inline_array_type_moves_into_nested_type() {
        assert_eq!(
            json!({
                "type": "record",
                "name": "test",
                "fields": [{
                    "name": "value",
                    "type": {"type": "array", "items": "int"},
                    "default": []
                }]
            }),
            expand(json!({
                "type": "record",
                "name": "test",
                "fields": [{"name": "value", "type": "array", "items": "int", "default": []}]
            }))
        );
    }

    #[test]
    fn inline_enum_type_takes_the_field_name() {
        assert_eq!(
            json!({
                "type": "record",
                "name": "Suit",
                "fields": [{
                    "name": "value",
                    "type": {"type": "enum", "name": "value", "symbols": ["SPADES", "CLUBS"]}
                }]
            }),
            expand(json!({
                "type": "record",
                "name": "Suit",
                "fields": [{"name": "value", "type": "enum", "symbols": ["SPADES", "CLUBS"]}]
            }))
        );
    }

    #[test]
    fn logical_type_in_nested_record_moves_into_nested_type() {
        assert_eq!(
            json!({
                "name": "metadata",
                "type": {
                    "type": "record",
                    "name": "metadata",
                    "fields": [{
                        "name": "timestamp",
                        "type": {"type": "long", "logicalType": "timestamp-micros"}
                    }]
                }
            }),
            expand(json!({
                "name": "metadata",
                "type": {
                    "type": "record",
                    "name": "metadata",
                    "fields": [{
                        "name": "timestamp",
                        "type": "long",
                        "logicalType": "timestamp-micros"
                    }]
                }
            }))
        );
    }

    /// Returns a record schema called `event` with the one field `field`.
    fn event_record(field: JsonValue) -> JsonValue {
        json!({"type": "record", "name": "event", "fields": [field]})
    }

    #[test]
    fn shorthand_inside_array_items_and_union_is_expanded() {
        let shorthand = event_record(json!({"name": "day", "type": "int", "logicalType": "date"}));
        let expanded = event_record(json!({
            "name": "day",
            "type": {"type": "int", "logicalType": "date"}
        }));

        assert_eq!(
            json!({"type": "array", "items": ["null", expanded]}),
            expand(json!({"type": "array", "items": ["null", shorthand]}))
        );
    }
}

# Schema Registry

Nisshi has a schema registry that supports [JSON schema](json-schema-org),
[Apache Avro](https://avro.apache.org) and [Protocol buffers](protocol-buffers),
that is embedded in the broker.

The schema registry is enabled using the `--schema-registry` command-line
parameter to the broker. A schema registry is a `file` or `s3` URL containing
schemas used to validate messages.
When a message is produced to a topic with an associated schema,
the message is validated against the schema. If a message does not conform to its
schema it is rejected by the broker with an `INVALID_RECORD` error.
An Apache Avro key or value must be an Avro object container whose `avro.codec`
is `null` or absent. The broker rejects a container with any other codec, such as
`deflate`, `snappy` or `zstandard`, with `INVALID_RECORD`.

Each schema is found in the root directory of the registry, named after the topic.
A `.json` extension is used for JSON schema, `.avsc` for Apache Avro, and  `.proto` for Protocol buffers.
For example the JSON schema for the person topic would be stored in `person.json`,
or `person.avsc` for an Apache Avro schema, `person.proto` for protocol buffer schema.

All schema types require a definition of the "Key" and/or "Value" of the Kafka message.

## JSON schema

An example JSON schema for the "person" topic.
This schema is stored in the root directory of the registry as `person.json`:

```json
{
  "title": "Person",
  "type": "object",
  "properties": {

    "key": {
      "type": "string",
      "pattern": "^[A-Z]{3}-\\d{3}$"
    },

    "value": {
      "type": "object",
      "properties": {
        "firstName": {
          "type": "string",
          "description": "The person's first name."
        },
        "lastName": {
          "type": "string",
          "description": "The person's last name."
        },
        "age": {
          "description": "Age in years which must be equal to or greater than zero.",
          "type": "integer",
          "minimum": 0
        }
      }
    }
  }
}
```

The schema must be an object, with properties for the message "key" and/or "value".

A schema that covers the message key, but allows any message value could look like:

```json
{
  "title": "Person",
  "type": "object",
  "properties": {
    "key": {
      "type": "string",
      "pattern": "^[A-Z]{3}-\\d{3}$"
    },
  }
}
```

A schema that allows any message key, but restricts the message value could look like:

```json
{
  "title": "Person",
  "type": "object",
  "properties": {
    "value": {
      "type": "object",
      "properties": {
        "firstName": {
          "type": "string",
          "description": "The person's first name."
        },
        "lastName": {
          "type": "string",
          "description": "The person's last name."
        },
        "age": {
          "description": "Age in years which must be equal to or greater than zero.",
          "type": "integer",
          "minimum": 0
        }
      }
    }
  }
}
```

## Protocol Buffers

An example protocol buffer schema for the "employee" topic.
This schema is stored in the root directory of the registry as `employee.proto`:

```protobuf
syntax = 'proto3';

message Key {
  int32 id = 1;
}

message Value {
  string name = 1;
  string email = 2;
}
```

The schema should contain message definitions for the `Key` and/or `Value`.

A schema that covers the message key, but allows any message value could look like:

```protobuf
syntax = 'proto3';

message Key {
  int32 id = 1;
}
```

A schema that allows any message key, but restricts the message value could look like:

```protobuf
syntax = 'proto3';

message Value {
  string name = 1;
  string email = 2;
}
```

### Example

This example uses JSON schema as it is simpler to use with the Apache Kafka command line tools.

The person schema can be found in the `etc/schema` directory of the Nisshi GitHub
repository. This directory is also used when starting Nisshi using
the `just nisshi-broker` recipe or Docker compose.

Starting Nisshi with schema validation enabled:

```shell
target/debug/nisshi broker --schema-registry file://./etc/schema 2>&1 | tee nisshi.log
```

Create the person topic:

```shell
target/debug/nisshi topic create person
```

The command prints:

```shell
Created topic person with 3 partitions.
```

Produce a message that is valid for the person schema:

```shell
echo '{"key": "345-67-6543", "value": {"firstName": "John", "lastName": "Doe", "age": 21}}' | target/debug/nisshi cat produce person
```

The command prints:

```shell
Produced 1 message to person partition 0, offset 0.
```

Produce a message that is invalid for the person schema (the `age` must be greater to equal to 0):

```shell
echo '{"key": "567-89-8765", "value":	{"firstName": "John", "lastName": "Doe", "age": -1}}' | ./target/debug/nisshi cat produce person
```

The command prints no confirmation, exits with status 4 (the broker refused the request, see the [exit codes](commands.md#exit-codes)), and prints the error that the broker returned:

```shell
2024-12-19T11:51:28.408011Z ERROR nisshi: 57: This record has failed the validation on broker and hence will be rejected.
```

The server log contains the reason for the message being rejected:

```shell
2024-12-19T11:51:28.407467Z DEBUG peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: nisshi_schema::json: 47: instance=Object {"code": String("ABC-123")}
2024-12-19T11:51:28.407524Z DEBUG peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: nisshi_schema::json: 56: r=()
2024-12-19T11:51:28.407546Z DEBUG peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: nisshi_schema::json: 40: validator=Some(Validator { root: SchemaNode { validators: Keyword, location: Location(""), absolute_path: Some(Uri { scheme: "https", authority: Some(Authority { userinfo: None, host: "example.com", host_parsed: RegName("example.com"), port: None }), path: "/person.schema.json", query: None, fragment: None }) }, config: CompilationConfig { draft: None, content_media_type: [], content_encoding: [] } }) encoded=Some(b"{\"firstName\": \"John\", \"lastName\": \"Doe\", \"age\": -1}")
2024-12-19T11:51:28.407589Z DEBUG peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: nisshi_schema::json: 47: instance=Object {"age": Number(-1), "firstName": String("John"), "lastName": String("Doe")}
2024-12-19T11:51:28.407626Z  WARN peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: nisshi_schema::json: 51: err=ValidationError { instance: Number(-1), kind: Minimum { limit: Number(0) }, instance_path: Location("/age"), schema_path: Location("/properties/age/minimum") }
2024-12-19T11:51:28.407652Z  WARN peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: nisshi_schema::json: 57: err=Api(InvalidRecord)
2024-12-19T11:51:28.407724Z  WARN peer{addr=127.0.0.1:60095}:produce{api_key=0 api_version=11 correlation_id=5}: nisshi_broker::broker::produce: 75: err=Storage(Api(InvalidRecord))
```

[json-schema-org]: https://json-schema.org/

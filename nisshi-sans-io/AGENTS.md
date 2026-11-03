# Research: nisshi-sans-io Crate

## Overview

`nisshi-sans-io` is the Kafka wire protocol implementation at the heart of Nisshi. As its name implies, it performs **no I/O** — it operates purely on bytes, converting between raw Kafka protocol frames and typed Rust structures. This makes it suitable for embedding in any async runtime or network stack (Nisshi uses `rama`).

The crate's description from `Cargo.toml`: *"A Kafka protocol implementation using serde"*.

The key insight of the design is that the entire Kafka binary protocol — serialization, deserialization, versioning, tagged fields — is expressed through serde's data model. A build-time code generator reads Apache Kafka's official JSON message descriptors and produces Rust types that `#[derive(Serialize, Deserialize)]`. Custom serde `Serializer` (`Encoder`) and `Deserializer` (`Decoder`) implementations then map between serde's abstract data model and Kafka's binary wire format.

## Source Layout

```
nisshi-sans-io/
├── build.rs                 # Code generator (1,276 lines)
├── Cargo.toml
├── message/                 # 185 official Kafka JSON descriptors
│   ├── ProduceRequest.json
│   ├── ProduceResponse.json
│   ├── FetchRequest.json
│   └── ... (185 files total)
├── src/
│   ├── lib.rs               # Frame, Header, Body, Error, traits (2,128 lines)
│   ├── ser.rs               # Encoder: serde Serializer → Kafka bytes (820 lines)
│   ├── de.rs                # Decoder: Kafka bytes → serde Deserializer (1,448 lines)
│   ├── primitive.rs         # ByteSize trait
│   ├── primitive/
│   │   ├── varint.rs        # VarInt, LongVarInt, UnsignedVarInt (zigzag encoding)
│   │   ├── tagged.rs        # TagBuffer, TagField (flexible version support)
│   │   ├── tagged/ser.rs    # Tagged field serializer
│   │   ├── tagged/de.rs     # Tagged field deserializer
│   │   └── uuid.rs          # UUID (128-bit) type
│   ├── record.rs            # Record module facade + Record struct + Builder
│   └── record/
│       ├── codec.rs         # Octets, Sequence, VarIntSequence codecs
│       ├── header.rs        # Record Header (key/value byte pairs)
│       ├── deflated.rs      # Compressed record batches (wire format)
│       └── inflated.rs      # Decompressed record batches (in-memory)
├── tests/
│   ├── it/                  # One test binary: main.rs + 18 test modules
│   │   ├── main.rs          # Declares every test module
│   │   ├── encode.rs        # Encoding tests
│   │   ├── decode.rs        # Decoding tests
│   │   ├── codec.rs         # Round-trip tests
│   │   ├── api.rs           # API-level tests
│   │   ├── snappy.rs        # Snappy compression tests
│   │   └── ...
│   └── proptest-regressions/ # Failure seeds proptest replays (proptest.txt)
└── benches/                 # Criterion benchmarks
```

## Build-Time Code Generation (`build.rs`)

### Input: Kafka JSON Message Descriptors

The `message/` directory contains 185 JSON files taken directly from the Apache Kafka source tree. Each file describes one API message (request or response). Example structure (`CreateTopicsRequest.json`):

```json
{
  "apiKey": 19,
  "type": "request",
  "listeners": ["zkBroker", "broker", "controller"],
  "name": "CreateTopicsRequest",
  "validVersions": "0-7",
  "deprecatedVersions": "0-1",
  "flexibleVersions": "5+",
  "fields": [
    {
      "name": "Topics",
      "type": "[]CreatableTopic",
      "versions": "0+",
      "about": "The topics to create.",
      "fields": [
        { "name": "Name", "type": "string", "versions": "0+", ... },
        { "name": "NumPartitions", "type": "int32", "versions": "0+", ... },
        ...
      ]
    },
    { "name": "TimeoutMs", "type": "int32", "versions": "0+", ... },
    { "name": "ValidateOnly", "type": "bool", "versions": "1+", ... }
  ]
}
```

Key descriptor properties:
- **`apiKey`**: 16-bit integer identifying the API (e.g., 0=Produce, 1=Fetch, 19=CreateTopics)
- **`type`**: `"request"` or `"response"`
- **`listeners`**: Which broker types handle this message (the generator filters for `"broker"`)
- **`validVersions`**: Range of supported protocol versions (e.g., `"0-7"`)
- **`flexibleVersions`**: Versions that support tagged fields (e.g., `"5+"`)
- **`fields`**: Array of field definitions, each with its own version range, type, and optional sub-fields

### Generation Pipeline

The `build.rs` (1,276 lines) uses `proc_macro2`, `quote`, and `syn` to generate Rust source at compile time:

1. **Read**: `read_value()` strips `//` comments from JSON files and parses them via `serde_json`
2. **Parse**: Converts JSON values into `nisshi_model::Message` structs (which model version ranges, field metadata, nested structures)
3. **Filter**: Only processes messages where `listeners` includes `"broker"` and an `apiKey` is defined
4. **Generate**: For each message, produces:

#### Generated Artifacts

**A. The `Body` enum** — A single enum with one variant per message type:
```rust
#[non_exhaustive]
pub enum Body {
    ProduceRequest(ProduceRequest),
    ProduceResponse(ProduceResponse),
    FetchRequest(FetchRequest),
    // ... ~90 variants
}
```
Plus `From<MessageType> for Body` and `TryFrom<Body> for MessageType` impls for each variant.

**B. Per-message modules and structs** — For each message (e.g., `CreateTopicsRequest`):
```rust
pub mod create_topics_request {
    pub struct CreatableTopic { ... }
    pub struct CreatableTopicConfig { ... }
    // nested types from the descriptor
}

pub struct CreateTopicsRequest {
    pub topics: Option<Vec<create_topics_request::CreatableTopic>>,
    pub timeout_ms: i32,
    pub validate_only: Option<bool>,  // Option because only valid in versions 1+
}
```

**C. `MESSAGE_META` static array** — Runtime metadata for the serializer/deserializer:
```rust
pub static MESSAGE_META: &[(&str, &MessageMeta)] = &[
    ("CreateTopicsRequest", &MessageMeta {
        api_key: 19,
        message_kind: MessageKind::Request,
        valid_versions: (0, 7),
        flexible_versions: Some(5),
        fields: &[
            ("topics", &FieldMeta { versions: (0, i16::MAX), ... }),
            ("timeout_ms", &FieldMeta { versions: (0, i16::MAX), ... }),
            // ...
        ],
    }),
    // ... one entry per message
];
```

### The Dual-Type Pattern (Public vs. Mezzanine)

A crucial design decision: each message type is generated **twice**:

1. **Public types** (in the crate's public API): Clean structs without protocol internals
2. **Mezzanine types** (in `mezzanine` module): Include a `tag_buffer: Option<TagBuffer>` field for flexible version support

The public types use `#[serde(try_from = "mezzanine::Type")]` and `#[serde(into = "mezzanine::Type")]` attributes so that serde automatically converts through the mezzanine layer during serialization/deserialization. This keeps the public API clean while the mezzanine types handle protocol-level details like tag buffers.

```rust
// Public (what users see):
pub struct CreateTopicsRequest {
    pub topics: Option<Vec<CreatableTopic>>,
    pub timeout_ms: i32,
    pub validate_only: Option<bool>,
}

// Mezzanine (internal, what serde actually serializes):
mod mezzanine {
    pub struct CreateTopicsRequest {
        pub topics: Option<Vec<CreatableTopic>>,
        pub timeout_ms: i32,
        pub validate_only: Option<bool>,
        pub tag_buffer: Option<TagBuffer>,  // <-- extra field
    }
}
```

### Version-Aware Field Generation

Fields are generated as `Option<T>` when they don't exist in all versions of a message. For example, if `validate_only` has `"versions": "1+"` in a message with `"validVersions": "0-7"`, it becomes `Option<bool>` because version 0 doesn't include it. The serializer/deserializer uses `MESSAGE_META` at runtime to determine whether to read/write each field based on the negotiated API version.

## Core Types (`lib.rs`)

### Frame

The top-level wire format unit:

```rust
pub struct Frame {
    pub size: i32,      // 4-byte big-endian length prefix
    pub header: Header, // Request or Response header
    pub body: Body,     // The actual message content
}
```

Methods:
- `Frame::request(header, body) -> Result<Bytes>` — Serialize a request frame
- `Frame::request_from_bytes(bytes) -> Result<Frame>` — Deserialize a request frame
- `Frame::response(header, body, api_key, api_version) -> Result<Bytes>` — Serialize a response
- `Frame::response_from_bytes(bytes, api_key, api_version) -> Result<Frame>` — Deserialize a response

Note the asymmetry: requests are self-describing (api_key and api_version are in the header), but responses require the caller to provide api_key and api_version because the response header only contains a correlation_id.

### Header

```rust
pub enum Header {
    Request {
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    },
    Response {
        correlation_id: i32,
    },
}
```

Like message types, `Header` has a `HeaderMezzanine` counterpart that includes `tag_buffer` fields for flexible versions.

### Traits

```rust
pub trait ApiKey { const KEY: i16; }
pub trait ApiName { const NAME: &'static str; }

pub trait Request: ApiKey + ApiName + Debug + Default + Into<Body> + Send + Sync + TryFrom<Body> + 'static {
    type Response: Response;
}

pub trait Response: ApiKey + ApiName + Debug + Default + Into<Body> + Send + Sync + TryFrom<Body> + 'static {
    type Request: Request;
}
```

The `Request` and `Response` traits pair each request with its corresponding response type at the type level, enabling compile-time safety in the broker's handler dispatch.

### RootMessageMeta

A lazily-initialized singleton (`OnceLock`) that indexes `MESSAGE_META` into two `HashMap<i16, &MessageMeta>` maps — one for requests, one for responses — keyed by API key. Used by the `Encoder` and `Decoder` to look up the schema for a given API key and version.

### Error and ErrorCode

`Error` is a `Clone`-able enum (non-Clone inner errors like `io::Error` are wrapped in `Arc`). It implements both `serde::ser::Error` and `serde::de::Error`, allowing it to flow through serde's error machinery.

`ErrorCode` is a large enum mapping every Kafka error code (e.g., `UnknownServerError`, `OffsetOutOfRange`, `CorruptMessage`, `GroupAuthorizationFailed`, etc.) with `i16` discriminants matching the Kafka protocol specification.

## Serialization (`ser.rs` — Encoder)

The `Encoder` struct implements serde's `Serializer` trait, mapping serde's data model to Kafka's binary protocol:

```rust
pub struct Encoder<'a> {
    writer: &'a mut dyn Write,
    containers: VecDeque<Container>,
    kind: Option<Kind>,      // Request or Response
    api_key: Option<i16>,
    api_version: Option<i16>,
    meta: Meta,
}
```

### How It Works

1. **Construction**: `Encoder::request(writer)` or `Encoder::response(writer, api_key, api_version)`. For requests, api_key/api_version are discovered during serialization from the Header. For responses, they must be provided upfront.

2. **Container Stack**: As serde traverses the struct tree (calling `serialize_struct`, `serialize_field`, etc.), the Encoder maintains a stack of `Container` entries tracking which struct is currently being serialized.

3. **Metadata Lookup**: When `serialize_struct` is called with a type name (e.g., `"CreateTopicsRequest"`), the Encoder looks it up in `RootMessageMeta` to find the `MessageMeta`. For each field within the struct, it looks up the `FieldMeta` to determine:
   - Whether the field exists in the current API version
   - How to encode it (compact string vs. regular string, compact array vs. regular array, etc.)
   - Whether it has tagged field semantics

4. **Type-Specific Encoding**: The Kafka protocol uses big-endian encoding for fixed-width integers. Strings have a 16-bit length prefix (or compact encoding with unsigned varint length in flexible versions). Arrays have a 32-bit length prefix (or compact encoding). Nullable types use -1 as a sentinel for null.

### Key Encoding Rules (Version-Dependent)

| Kafka Type | Non-Flexible Encoding | Flexible Encoding |
|------------|----------------------|-------------------|
| `string` | 2-byte BE length prefix + UTF-8 | UnsignedVarInt length + 1, UTF-8 |
| `bytes` | 4-byte BE length prefix + data | UnsignedVarInt length + 1, data |
| `[]T` (array) | 4-byte BE count + elements | UnsignedVarInt count + 1, elements |
| `int8`..`int64` | Big-endian fixed width | Same |
| `bool` | 1 byte (0/1) | Same |
| `uuid` | 16 bytes raw | Same |
| Null string | Length = -1 (0xFFFF) | Length = 0 |
| Null array | Length = -1 | Length = 0 |

## Deserialization (`de.rs` — Decoder)

The `Decoder` struct implements serde's `Deserializer` trait:

```rust
pub struct Decoder<'de> {
    reader: &'de mut dyn Read,
    containers: VecDeque<Container>,
    field: Option<&'static str>,
    kind: Option<Kind>,
    api_key: Option<i16>,
    api_version: Option<i16>,
    meta: Meta,
    length: Option<usize>,
    in_seq_of_primitive: bool,
    path: VecDeque<&'static str>,
    in_records: bool,
}
```

### How It Works

1. **Construction**: `Decoder::request(reader)` or `Decoder::response(reader, api_key, api_version)`.

2. **Self-Discovery (Requests)**: For requests, the Decoder reads the 4-byte frame size, then the 2-byte api_key and 2-byte api_version from the header. It uses these to look up the message schema in `RootMessageMeta`, determining which `Body` variant to expect and what fields to decode.

3. **Field-by-Field Decoding**: As serde's derive macro drives the deserialization (calling `deserialize_struct`, `deserialize_identifier`, etc.), the Decoder:
   - Checks `FieldMeta` to determine if the current field exists in this API version
   - If the field doesn't exist, returns `None` (for `Option<T>` fields)
   - Otherwise reads the appropriate number of bytes and decodes them

4. **Path Tracking**: The Decoder maintains a `path: VecDeque<&str>` tracking the current position in the struct tree (e.g., `["CreateTopicsRequest", "topics", "CreatableTopic"]`). This is used for metadata lookups in nested structures.

5. **Special Record Handling**: The `in_records` flag triggers special decoding logic for Kafka record batches, which have their own binary format distinct from the API message format.

## Primitive Types (`primitive/`)

### VarInt and LongVarInt (Zigzag Encoding)

Kafka uses Protocol Buffers-style zigzag variable-length integers in record batches:

```rust
// Zigzag encoding: maps signed integers to unsigned
// 0 → 0, -1 → 1, 1 → 2, -2 → 3, 2 → 4, ...
fn en_zigzag(decoded: i32) -> u32 {
    ((decoded << 1) ^ (decoded >> 31)) as u32
}

fn de_zigzag(encoded: u32) -> i32 {
    ((encoded >> 1) as i32) ^ -((encoded & 1) as i32)
}
```

The variable-length encoding uses 7 bits per byte with the high bit as a continuation flag (same as protobuf varint). Three types:
- `VarInt` — zigzag-encoded i32
- `LongVarInt` — zigzag-encoded i64
- `UnsignedVarInt` — unsigned u32 (no zigzag, used for lengths in flexible versions)

Each type provides both standalone `Encode`/`Decode` implementations and serde `serialize`/`deserialize` functions usable via `#[serde(serialize_with = ...)]`.

### Tagged Fields (Flexible Versions)

Starting from "flexible versions", Kafka messages can include optional tagged fields — a mechanism for backward-compatible schema evolution similar to protobuf's field tags.

```rust
pub struct TagField(pub u32, pub Vec<u8>);  // (tag_id, data)
pub struct TagBuffer(pub Vec<TagField>);
```

A `TagBuffer` is serialized as:
1. `UnsignedVarInt` — number of tagged fields
2. For each field: `UnsignedVarInt` tag ID, `UnsignedVarInt` data length, raw bytes

The mezzanine types include `tag_buffer: Option<TagBuffer>` which is `Some` in flexible versions and `None` otherwise. The serializer/deserializer handles this transparently.

## Record Batches (`record/`)

Record batches are the payload format for Produce/Fetch operations. They have their own binary format, distinct from the API message framing.

### Record

A single Kafka record within a batch:

```rust
pub struct Record {
    pub length: i32,           // varint-encoded
    pub attributes: u8,
    pub timestamp_delta: i64,  // long varint, delta from batch base
    pub offset_delta: i32,     // varint, delta from batch base offset
    pub key: Option<Bytes>,    // varint-prefixed nullable bytes
    pub value: Option<Bytes>,  // varint-prefixed nullable bytes
    pub headers: Vec<Header>,  // varint-prefixed array
}
```

Records use varint encoding throughout (not big-endian fixed-width), and timestamps/offsets are stored as deltas from the batch's base values.

Records include a `Builder` for ergonomic construction:
```rust
let record = Record::builder()
    .key(Some(Bytes::from_static(b"key")))
    .value(Some(Bytes::from_static(b"value")))
    .header(Header::builder()
        .key(Bytes::from_static(b"format"))
        .value(Bytes::from_static(b"json")))
    .build()?;
```

### Deflated Batch (Wire Format)

`deflated::Batch` represents a record batch as it appears on the wire — records are compressed:

```rust
pub struct Batch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,                    // always 2 for current format
    pub crc: u32,                     // CRC32-C of everything after this field
    pub attributes: i16,              // compression, timestamp type, etc.
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: u32,
    pub record_data: Bytes,           // compressed record data
}
```

The fixed header is 49 bytes (`FIXED_BATCH_LENGTH`), followed by the compressed record data.

### Inflated Batch (In-Memory)

`inflated::Batch` holds decompressed records for application use:

```rust
pub struct Batch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,  // decompressed records
}
```

### Compression

Bidirectional conversion between deflated and inflated batches:
- `deflated::Batch::try_from(inflated::Batch)` — compresses records
- `inflated::Batch::try_from(&deflated::Batch)` — decompresses records

Supported compression algorithms (from `Compression` enum):
- **None** — uncompressed
- **Gzip** — via `flate2`
- **Snappy** — via `snap`
- **LZ4** — via `lz4`
- **ZStandard** — via `zstd`

The compression type is stored in bits 0-2 of the batch `attributes` field. CRC32-C checksums (via `crc-fast`) verify batch integrity.

## Wire Protocol Flow

### Encoding a Request

```
Application code
    → Frame::request(header, body)
        → Frame { size: 0, header, body }.serialize(&mut Encoder::request(writer))
            → Encoder writes 4-byte placeholder size
            → Encoder writes Header::Request fields (api_key, api_version, correlation_id, client_id)
            → Encoder recognizes Body variant, looks up MESSAGE_META
            → Encoder writes each field per FieldMeta for the negotiated version
            → Encoder patches size at position 0
        → Returns Bytes
```

### Decoding a Request

```
Raw bytes from network
    → Frame::request_from_bytes(bytes)
        → Frame::deserialize(&mut Decoder::request(reader))
            → Decoder reads 4-byte size
            → Decoder reads api_key (2 bytes) and api_version (2 bytes) from header
            → Decoder looks up MESSAGE_META for this api_key
            → Decoder reads correlation_id, client_id
            → Decoder determines Body variant from api_key
            → Decoder reads each field per FieldMeta, skipping fields not in this version
            → For flexible versions, reads and discards/stores tag buffers
        → Returns Frame { size, header: Header::Request{...}, body: Body::SomeMessage{...} }
```

### Response Encoding/Decoding

Responses follow the same pattern but require explicit `api_key` and `api_version` parameters because the response header only contains `correlation_id`. The caller (typically the broker) must track which request a response corresponds to.

## Dependencies

**Runtime:**
- `bytes` — Zero-copy byte buffers (`Bytes`, `BytesMut`)
- `serde` — Serialization framework (derive + custom Serializer/Deserializer)
- `flate2`, `lz4`, `snap`, `zstd` — Compression codecs
- `crc-fast` — CRC32-C checksums for record batch integrity
- `rama` — Async service framework (for network layer integration)
- `nisshi-model` — Kafka message metadata model (`MessageMeta`, `FieldMeta`, `Message`)
- `tracing` — Structured logging/instrumentation

**Build-time:**
- `proc-macro2`, `quote`, `syn` — Rust code generation
- `serde_json` — JSON descriptor parsing
- `glob` — File pattern matching
- `convert_case` — PascalCase ↔ snake_case conversion
- `prettyplease` — Generated code formatting
- `nisshi-model` — Parses Kafka JSON descriptors into structured metadata

## Design Principles

1. **Sans-I/O**: The crate never performs network I/O. It converts between `Bytes` and typed Rust structs, leaving networking to the caller. This enables testing without network infrastructure and embedding in any async runtime.

2. **Code Generation from Upstream Sources**: Rather than hand-writing protocol types, the build script reads the same JSON descriptors used by the official Kafka codebase. This ensures protocol correctness and makes it straightforward to track upstream protocol changes by updating the JSON files.

3. **Serde as the Protocol Engine**: By implementing custom `Serializer`/`Deserializer` that speak Kafka's binary format, the crate leverages serde's derive macros for free. The generated types just `#[derive(Serialize, Deserialize)]` and get correct Kafka protocol encoding/decoding.

4. **Version-Aware Serialization**: A single set of Rust types covers all protocol versions. The `MESSAGE_META` table drives version-specific field inclusion at runtime, avoiding separate types per version.

5. **Public API Cleanliness**: The mezzanine/public type split keeps protocol details (tag buffers) out of the consumer-facing API while maintaining full protocol fidelity internally.

6. **Builder Pattern**: All generated message types support fluent builders, making test construction ergonomic.

7. **Clone-able Errors**: All `Error` variants implement `Clone` (with `Arc` wrappers for non-Clone inner errors), enabling error types to flow through serde's error model and be used in contexts requiring `Clone`.

8. **No `unsafe` Code**: The crate forbids `unsafe_code` via `#![forbid(unsafe_code)]` in the workspace lints.

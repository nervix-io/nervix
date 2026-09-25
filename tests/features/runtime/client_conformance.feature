Feature: Cross-language client conformance
  Every client runtime reads the same rows, answers and failures through the finalized session
  protocol and prints the same report, so one expected report is the oracle for every language.
  The shared Rust binding is driven from C, C++, Python, Java and Ruby, and in process through
  its C ABI; Go over native gRPC and TypeScript on Node.js and Bun over the binary WebSocket are
  independent implementations generated from the schema.

  The rows cover the extremes of every integer width, 64-bit values on both sides of the
  JavaScript safe-integer boundary, an absent and a present-zero optional value, strings with
  multi-byte characters and an embedded NUL, bytes that are not UTF-8, empty strings and bytes,
  float sign and subnormal bits, the extreme DATETIME nanoseconds, a redacted sensitive field and
  two interleaved concrete branches. Strings and bytes are reported as hex, floats as their bits.

  Scenario Outline: A <runtime> client round-trips an operation, typed rows, an error and a closure
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA typed_row (
        id U32,
        tenant STRING,
        u8v U8,
        i8v I8,
        u16v U16,
        i16v I16,
        u32v U32,
        i32v I32,
        u64v U64,
        i64v I64,
        f32v F32,
        f64v F64,
        flag BOOL,
        text STRING,
        raw BYTES,
        at DATETIME,
        maybe I64 OPTIONAL,
        secret STRING SENSITIVE
      );
      CREATE WIRE JSON SCHEMA typed_row_wire MODE STRICT (
        id integer,
        tenant string,
        u8v integer,
        i8v integer,
        u16v integer,
        i16v integer,
        u32v integer,
        i32v integer,
        u64v integer,
        i64v integer,
        f32v number,
        f64v number,
        flag boolean,
        text string,
        raw BYTES,
        at string,
        maybe integer OPTIONAL,
        secret string
      );
      CREATE CODEC typed_row_codec
        FROM WIRE JSON SCHEMA typed_row_wire
        TO SCHEMA typed_row
        ENCODE at AS RFC3339;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY typed_rows SCHEMA typed_row BRANCHED BY by_tenant;
      CREATE VHOST edge conformance-{{test_id}}.example.com;
      CREATE ENDPOINT typed_ingress ON edge PATH '/typed' TYPE HTTP;
      CREATE INGESTOR typed_source
        FROM ENDPOINT typed_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING typed_row_codec
        TO typed_rows
          SET id = message.id,
              tenant = message.tenant,
              u8v = message.u8v,
              i8v = message.i8v,
              u16v = message.u16v,
              i16v = message.i16v,
              u32v = message.u32v,
              i32v = message.i32v,
              u64v = message.u64v,
              i64v = message.i64v,
              f32v = message.f32v,
              f64v = message.f64v,
              flag = message.flag,
              text = message.text,
              raw = message.raw,
              at = message.at,
              maybe = message.maybe,
              secret = message.secret
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When the "<runtime>" client probe subscribes as "probe" to relay "typed_rows" on node "node-1" expecting 3 rows
    And http payload is posted to node "node-1" with host "conformance-{{test_id}}.example.com" path "/typed"
      """
      {"id":1,"tenant":"acme","u8v":255,"i8v":127,"u16v":65535,"i16v":32767,"u32v":4294967295,"i32v":2147483647,"u64v":18446744073709551615,"i64v":9223372036854775807,"f32v":3.4028235e38,"f64v":1.7976931348623157e308,"flag":true,"text":"h\u00e9llo \u4e16\u754c \ud83d\ude80","raw":"AP/+gAA=","at":"2262-04-11T23:47:16.854775807Z","secret":"s1"}
      """
    And http payload is posted to node "node-1" with host "conformance-{{test_id}}.example.com" path "/typed"
      """
      {"id":2,"tenant":"beta","u8v":0,"i8v":-128,"u16v":0,"i16v":-32768,"u32v":0,"i32v":-2147483648,"u64v":0,"i64v":-9223372036854775808,"f32v":-0.0,"f64v":5e-324,"flag":false,"text":"a\u0000b","raw":"","at":"1677-09-21T00:12:43.145224192Z","maybe":0,"secret":"s2"}
      """
    And http payload is posted to node "node-1" with host "conformance-{{test_id}}.example.com" path "/typed"
      """
      {"id":3,"tenant":"acme","u8v":1,"i8v":-1,"u16v":1,"i16v":-1,"u32v":1,"i32v":-1,"u64v":9007199254740993,"i64v":-9007199254740993,"f32v":1.5,"f64v":0.1,"flag":true,"text":"","raw":"AA==","at":"1970-01-01T00:00:00.000000001Z","maybe":9007199254740992,"secret":"s3"}
      """
    Then within "180s" the client probe reports
      """
      OPERATION completed
      ERROR failed diagnostics=1 span=12..12
      FIELD id U32 required public
      FIELD tenant STRING required public
      FIELD u8v U8 required public
      FIELD i8v I8 required public
      FIELD u16v U16 required public
      FIELD i16v I16 required public
      FIELD u32v U32 required public
      FIELD i32v I32 required public
      FIELD u64v U64 required public
      FIELD i64v I64 required public
      FIELD f32v F32 required public
      FIELD f64v F64 required public
      FIELD flag BOOL required public
      FIELD text STRING required public
      FIELD raw BYTES required public
      FIELD at DATETIME required public
      FIELD maybe I64 nullable public
      FIELD secret STRING required sensitive
      BRANCH by_tenant
      KEY tenant STRING required public
      SUBSCRIBED
      ROW [tenant=str:61636d65] id=u32:1 tenant=str:61636d65 u8v=u8:255 i8v=i8:127 u16v=u16:65535 i16v=i16:32767 u32v=u32:4294967295 i32v=i32:2147483647 u64v=u64:18446744073709551615 i64v=i64:9223372036854775807 f32v=f32:7f7fffff f64v=f64:7fefffffffffffff flag=bool:true text=str:68c3a96c6c6f20e4b896e7958c20f09f9a80 raw=bytes:00fffe8000 at=datetime:9223372036854775807 maybe=null secret=redacted
      ROW [tenant=str:62657461] id=u32:2 tenant=str:62657461 u8v=u8:0 i8v=i8:-128 u16v=u16:0 i16v=i16:-32768 u32v=u32:0 i32v=i32:-2147483648 u64v=u64:0 i64v=i64:-9223372036854775808 f32v=f32:80000000 f64v=f64:0000000000000001 flag=bool:false text=str:610062 raw=bytes: at=datetime:-9223372036854775808 maybe=i64:0 secret=redacted
      ROW [tenant=str:61636d65] id=u32:3 tenant=str:61636d65 u8v=u8:1 i8v=i8:-1 u16v=u16:1 i16v=i16:-1 u32v=u32:1 i32v=i32:-1 u64v=u64:9007199254740993 i64v=i64:-9007199254740993 f32v=f32:3fc00000 f64v=f64:3fb999999999999a flag=bool:true text=str: raw=bytes:00 at=datetime:1 maybe=i64:9007199254740992 secret=redacted
      CHECKS ok
      CLOSED completed
      PASS
      """

    Examples: the C ABI in process
      | runtime          | cluster_size |
      | c-abi-in-process | 1            |
      | c-abi-in-process | 3            |

    @client_conformance_toolchain @client_probe_c
    Examples: C over the shared Rust binding
      | runtime | cluster_size |
      | c       | 1            |
      | c       | 3            |

    @client_conformance_toolchain @client_probe_cpp
    Examples: C++ over the shared Rust binding
      | runtime | cluster_size |
      | c++     | 1            |
      | c++     | 3            |

    @client_conformance_toolchain @client_probe_python
    Examples: CPython over the shared Rust binding
      | runtime | cluster_size |
      | python  | 1            |
      | python  | 3            |

    @client_conformance_toolchain @client_probe_java
    Examples: Java over the shared Rust binding
      | runtime | cluster_size |
      | java    | 1            |
      | java    | 3            |

    @client_conformance_toolchain @client_probe_ruby
    Examples: Ruby over the shared Rust binding
      | runtime | cluster_size |
      | ruby    | 1            |
      | ruby    | 3            |

    @client_conformance_toolchain @client_probe_go
    Examples: an independent Go client over native gRPC
      | runtime | cluster_size |
      | go      | 1            |
      | go      | 3            |

    @client_conformance_toolchain @client_probe_node
    Examples: an independent TypeScript client on Node.js over the binary WebSocket
      | runtime | cluster_size |
      | node    | 1            |
      | node    | 3            |

    @client_conformance_toolchain @client_probe_bun
    Examples: the same TypeScript client on Bun
      | runtime | cluster_size |
      | bun     | 1            |
      | bun     | 3            |

  Scenario Outline: A <runtime> client reads every frame of the conformance corpus the Rust encoder wrote
    When the "<runtime>" client probe decodes the conformance corpus
    Then within "60s" the client probe reports the conformance corpus

    @client_conformance_toolchain @client_probe_go
    Examples: an independent Go reader
      | runtime |
      | go      |

    @client_conformance_toolchain @client_probe_node
    Examples: an independent TypeScript reader on Node.js
      | runtime |
      | node    |

    @client_conformance_toolchain @client_probe_bun
    Examples: the same TypeScript reader on Bun
      | runtime |
      | bun     |

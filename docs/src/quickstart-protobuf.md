# Protobuf Codecs

Protobuf payloads are decoded from real `.proto` definitions. The definitions are uploaded as a
versioned [resource](resources.md#lifecycle); the codec compiles them, decodes each payload into
the message's JSON view, and hands that to a JAQ program — the same bridge as the previous step.

## The Proto File

Create `proto/order_event.proto` in your working directory, with field names matching
`order_record`:

```proto
syntax = "proto3";

package quickstart;

message OrderEvent {
  string order_id = 1;
  string customer = 2;
  string status = 3;
  int64 amount = 4;
  int64 quantity = 5;
}
```

## Upload It As A Resource

`UPLOAD RESOURCE` is a client-side statement — the CLI reads the directory from your machine and
streams it to the cluster — so it cannot sit inside a transaction, and its path resolves relative
to where `nervix-cli` runs:

```nspl
BEGIN;
CREATE RESOURCE order_proto;
COMMIT;

UPLOAD RESOURCE order_proto VERSION './proto';
```

The client reports `uploaded resource version 1`. Uploads are immutable and versioned; see
[Versioning](resources.md#versioning).

## The Codec And Its Endpoint

The clause order is rigid: resource, `CONFIG` (mandatory — `file`/`include` select and root the
`.proto` files inside the resource), the fully-qualified `MESSAGE` name, then the target schema
and JAQ program. The message's JSON view keeps snake_case proto field names and numeric 64-bit
integers, so a field-for-field message needs only the identity program `'.'`:

```nspl
BEGIN;

CREATE CODEC order_protobuf_codec
  FROM PROTOBUF
  USING RESOURCE order_proto VERSION 1
  CONFIG {'file' = 'order_event.proto', 'include' = '.'}
  MESSAGE 'quickstart.OrderEvent'
  TO SCHEMA order_record
  WITH JAQ TRANSFORMATIONS ON INGESTION '.';

CREATE RELAY proto_orders SCHEMA order_record UNBRANCHED;

CREATE ENDPOINT proto_ingress ON edge PATH '/proto-orders' TYPE HTTP;

CREATE INGESTOR proto_order_source
  FROM ENDPOINT proto_ingress MODE NO_ACK SEQUENTIAL
  DECODE USING order_protobuf_codec
  TO proto_orders
    INHERIT ALL
    UNBRANCHED
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

COMMIT;
```

Because it declares only `ON INGESTION`, this codec is decode-only; encoding to protobuf requires
an `ON EMITTING` program. A `DATETIME` field would ride in a proto `string` plus
`ENCODE <field> AS RFC3339` on the codec ([Codecs](schemas-and-codecs.md#codecs)).

## Send A Binary Order

Watch the relay:

```bash
nervix-cli --domain quickstart subscribe proto_watch proto_orders
```

`protoc --encode` turns protobuf text format into wire bytes, which curl posts as the raw body:

```bash
protoc --encode=quickstart.OrderEvent proto/order_event.proto <<'EOF' > order.bin
order_id: "o-6001"
customer: "acme"
status: "new"
amount: 4200
quantity: 6
EOF

curl -i -X POST http://127.0.0.1:8080/proto-orders \
  -H 'Host: orders.example.com' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @order.bin
```

The decoded record appears in the subscription — typed, with no JSON anywhere on the wire.

Next: extend the expression language itself in
[User-Defined Functions](./quickstart-udfs.md).

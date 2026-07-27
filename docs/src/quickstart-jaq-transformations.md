# JAQ Transformations

So far every payload matched the wire schema field for field. Real feeds rarely do. When a partner
sends orders as a nested envelope, a JAQ codec reshapes the payload at the boundary instead of
declaring a wire schema: the codec parses the native format and runs a
[jq-style program](schemas-and-codecs.md#jaq-transformations) whose output must be one JSON object
matching the internal schema.

Suppose the partner posts this envelope:

```json
{"order":{"id":"o-5001","customer":"acme","state":"new"},"totals":{"amount":1200,"quantity":4}}
```

## A JAQ Codec

A JAQ codec replaces the wire schema entirely — it goes straight from the format to the internal
schema, with the program inline:

```nspl
BEGIN;

CREATE CODEC partner_order_codec
  FROM JSON
  TO SCHEMA order_record
  WITH JAQ TRANSFORMATION '{
    order_id: .order.id,
    customer: .order.customer,
    status: .order.state,
    amount: .totals.amount,
    quantity: .totals.quantity
  }';
```

The singular `WITH JAQ TRANSFORMATION` declares the **ingestion** direction only, so this codec
decodes but cannot encode. A bidirectional codec spells both programs out:
`WITH JAQ TRANSFORMATIONS ON INGESTION '...' ON EMITTING '...'`
([JAQ Transformations](schemas-and-codecs.md#jaq-transformations)). `FROM` accepts `JSON`, `YAML`,
`TOML`, `XML`, and `CBOR`.

Two rules to remember: any field the program can produce as `null` must be `OPTIONAL` in the
internal schema, and surplus keys in the program's output are silently dropped.

## Wire It To An Endpoint

Reuse the vhost from [HTTP Ingestion](./quickstart-http-ingestion.md) with a second path:

```nspl
CREATE RELAY partner_orders SCHEMA order_record UNBRANCHED;

CREATE ENDPOINT partner_ingress ON edge PATH '/partner-orders' TYPE HTTP;

CREATE INGESTOR partner_order_source
  FROM ENDPOINT partner_ingress MODE NO_ACK SEQUENTIAL
  DECODE USING partner_order_codec
  TO partner_orders
    INHERIT ALL
    UNBRANCHED
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

COMMIT;
```

## Send The Envelope

```bash
nervix-cli --domain quickstart subscribe partner_watch partner_orders
```

```bash
curl -i -X POST http://127.0.0.1:8080/partner-orders \
  -H 'Host: orders.example.com' \
  -H 'Content-Type: application/json' \
  -d '{"order":{"id":"o-5001","customer":"acme","state":"new"},"totals":{"amount":1200,"quantity":4}}'
```

The subscription shows the flattened `order_record` — the envelope never enters the graph.

Next: binary payloads in [Protobuf Codecs](./quickstart-protobuf.md).

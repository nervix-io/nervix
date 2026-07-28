# HTTP Ingestion

Orders do not have to come through a broker. Nervix hosts its own inbound HTTP endpoints: you
declare a virtual host and a path, attach an ingestor, and POST records straight at the server.
See [HTTP Endpoints](ingestors.md#http-endpoints).

Inbound endpoints are served by the node itself on its HTTP listener — `--http-listen-addr`,
`0.0.0.0:8080` by default (TLS-enabled vhosts use the separate HTTPS listener instead).

## Declare The Endpoint

Three statements, in order: the `VHOST` names the hostnames served, the `ENDPOINT` binds a path on
that vhost, and the ingestor consumes the endpoint. The existing `order_codec` decodes the same
JSON as the Kafka path:

```nspl
BEGIN;

CREATE RELAY orders_http SCHEMA order_record UNBRANCHED;

CREATE VHOST edge orders.example.com;

CREATE ENDPOINT orders_ingress
  ON edge
  PATH '/orders'
  TYPE HTTP;

CREATE INGESTOR http_orders
  FROM ENDPOINT orders_ingress MODE NO_ACK SEQUENTIAL
  DECODE USING order_codec
  TO orders_http
    INHERIT ALL
    UNBRANCHED
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

COMMIT;
```

`MODE NO_ACK SEQUENTIAL` is the only mode endpoint ingestors support — there is no broker to
acknowledge to. Each POST body is one record. To merge this stream with the Kafka-fed `orders`
relay, a [junction](processors.md#junction) can consume both relays with
`FROM orders, orders_http`.

## POST An Order

Watch the relay first:

```bash
nervix-cli --domain quickstart subscribe http_watch orders_http
```

Then send a record. The vhost is matched against the request's `Host` header (lowercased, port
stripped), so tell curl which host you mean:

```bash
curl -i -X POST http://127.0.0.1:8080/orders \
  -H 'Host: orders.example.com' \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"o-4001","customer":"acme","status":"new","amount":700,"quantity":7}'
```

The server answers `202 Accepted` and the record appears in the subscription. Requests to an
unknown host or path get `404`, and anything other than `POST` gets `405`.

One thing `202` does **not** mean: that the payload decoded. Dispatch is fire-and-forget — a body
that fails the codec goes to the route's `ON MESSAGE ERROR` policy, not back to the HTTP client.
Verify through the subscription, not the status code.

Next: reshape foreign JSON at the boundary in
[JAQ Transformations](./quickstart-jaq-transformations.md).

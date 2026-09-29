Feature: Client ingestors
  An application publishes typed Arrow batches to a client ingestor through producers it opens on
  a native gRPC session or a console WebSocket session. One submitted batch is one acknowledgement
  unit: it completes once every route and acknowledging sink its rows reached confirmed them. See
  docs/src/ingestors.md#client-ingestors and docs/src/client-library.md#producers.

  @client_ingestor
  Scenario Outline: Producers publish typed batches through transforming and branched routes to an acknowledging sink
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers with
      """
      respond 200; after 2s
      """
    And HTTP receiver "sink" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE SCHEMA order_record (
        region STRING, order_id STRING, amount_cents I64, card STRING SENSITIVE
      );
      CREATE SCHEMA region_key (region STRING);
      CREATE BRANCH by_region SCHEMA region_key TTL 5m;
      CREATE RELAY orders_by_region SCHEMA order_record BRANCHED BY by_region;
      CREATE RELAY orders_deduped SCHEMA order_record BRANCHED BY by_region;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders_by_region
          INHERIT region, order_id, card
          SET amount_cents = input.amount * 100
          BRANCHED BY by_region SET region = message.region
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedupe_orders
        FROM orders_by_region
        DEDUPLICATE ON input.order_id
        MAX TIME 5m
        BRANCHED BY by_region
        TO orders_deduped
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SCHEMA order_body (order_id STRING, amount_cents I64, card STRING SENSITIVE);
      CREATE WIRE JSON SCHEMA order_body_wire MODE STRICT (
        order_id string, amount_cents integer, card string
      );
      CREATE CODEC order_body_codec FROM WIRE JSON SCHEMA order_body_wire TO SCHEMA order_body;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders_deduped
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING order_body_codec
        INHERIT order_id, amount_cents
        SET card = leak_sensitive(input.card)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to node "node-1"
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE INGESTOR orders_in;
      """
    Then the last command output contains
      """
      FROM CLIENT SCHEMA order_in MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND
      """
    And <session> cannot open a producer on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING" because "schema mismatch"
    When <session> opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    Then within "30s" the leader node describes ingestor "orders_in" with
      """
      source: CLIENT
      schema: order_in
      mode: ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
      admission: open
      producers: 1
      """
    When producer "orders" submits batch "first" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 12     | 4111-1 |
      | us     | o-1      | 7      | 4111-2 |
      | eu     | o-2      | 5      | 4111-3 |
    # The receiver delays its first answer, so the batch completes only after its slowest row
    # reached the sink's success boundary.
    Then batch "first" completed at least "2s" after HTTP receiver "sink" request 1 arrived
    When producer "orders" submits batch "second" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 12     | 4111-9 |
      | us     | o-3      | 9      | 4111-4 |
    Then batch "second" completes
    # The repeated order o-1 is a duplicate only inside the eu branch.
    And HTTP receiver "sink" has captured exactly 4 requests
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/eu/o-1

      {"order_id":"o-1","amount_cents":1200,"card":"4111-1"}
      """
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/us/o-1

      {"order_id":"o-1","amount_cents":700,"card":"4111-2"}
      """
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/eu/o-2

      {"order_id":"o-2","amount_cents":500,"card":"4111-3"}
      """
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/us/o-3

      {"order_id":"o-3","amount_cents":900,"card":"4111-4"}
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW INGESTORS;
      """
    Then the last command output contains
      """
      orders_in source=CLIENT schema=order_in
      """
    When producer "orders" is closed
    Then within "30s" the leader node describes ingestor "orders_in" with
      """
      producers: 0
      outstanding batches: 0
      admitted batches: 0
      """

    Examples:
      | session                 | cluster_size |
      | client "app"            | 1            |
      | client "app"            | 3            |
      | WebSocket session "app" | 1            |
      | WebSocket session "app" | 3            |

  @client_ingestor
  Scenario Outline: A batch that is not the ingestor's canonical Arrow stream is refused whole, and an admitted row error follows its route policy
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE SCHEMA order_error (order_id STRING, code STRING);
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE RELAY order_errors SCHEMA order_error UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders
          INHERIT ALL
          SET amount = input.amount * 100
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR routed_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders
          INHERIT ALL
          SET amount = input.amount * 100
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO order_errors
            SET order_id = input.order_id,
                code = error.code
        ON GENERAL ERROR LOG;
      CREATE INGESTOR lenient_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders
          INHERIT ALL
          SET amount = input.amount * 100
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR IGNORE
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER diverted FROM order_errors
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/errors/', input.order_id, '/', input.code)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to node "node-1"
    When <session> opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "orders" submits batch "garbage" that is "not an Arrow stream"
    Then batch "garbage" is not admitted because "invalid batch: malformed"
    When producer "orders" submits batch "foreign" that is "of another schema"
    Then batch "foreign" is not admitted because "invalid batch: schema mismatch"
    When producer "orders" submits batch "doubled" that is "two record batches"
    Then batch "doubled" is not admitted because "invalid batch: not one batch"
    # The second row's amount overflows its route assignment. The route's ON MESSAGE ERROR policy
    # handles that row exactly as it does for any ingestor: LOG negatively acknowledges it, so the
    # batch fails processing, while the other row is still published.
    When producer "orders" submits batch "overflowing" with rows
      | region | order_id | amount              | card   |
      | eu     | o-1      | 3                   | 4111-1 |
      | eu     | o-2      | 9223372036854775807 | 4111-2 |
    Then batch "overflowing" fails processing because "rejected"
    And HTTP receiver "sink" eventually receives at least 1 request
    And HTTP receiver "sink" request 1 is
      """
      POST /orders/eu/o-1
      """
    # IGNORE acknowledges the failed row instead, so the same batch completes once the sink
    # acknowledges its other row.
    When <session> opens producer "lenient" on ingestor "lenient_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "lenient" submits batch "forgiven" with rows
      | region | order_id | amount              | card   |
      | eu     | o-3      | 4                   | 4111-3 |
      | eu     | o-4      | 9223372036854775807 | 4111-4 |
    Then batch "forgiven" completes
    And HTTP receiver "sink" has captured exactly 2 requests
    And HTTP receiver "sink" request 2 is
      """
      POST /orders/eu/o-3
      """
    # SEND TO acknowledges the failed row once its error record is published to the error relay,
    # whose own emitter delivers it like any other record.
    When <session> opens producer "routed" on ingestor "routed_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "routed" submits batch "diverted" with rows
      | region | order_id | amount              | card   |
      | eu     | o-5      | 5                   | 4111-5 |
      | eu     | o-6      | 9223372036854775807 | 4111-6 |
    Then batch "diverted" completes
    And HTTP receiver "sink" eventually receives at least 4 requests
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/eu/o-5
      """
    And HTTP receiver "sink" captured one request that is
      """
      POST /errors/o-6/evaluation
      """

    Examples:
      | session                 | cluster_size |
      | client "app"            | 1            |
      | WebSocket session "app" | 3            |

  @client_ingestor
  Scenario Outline: A producer is refused what it cannot attach to and ends with its ingestor's lifecycle
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT (
        region string, order_id string, amount integer, card string
      );
      CREATE CODEC order_codec FROM WIRE JSON SCHEMA order_wire TO SCHEMA order_in;
      CREATE VHOST edge orders-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/orders' TYPE HTTP;
      CREATE INGESTOR orders_http
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING order_codec
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to node "node-1"
    Then <session> cannot open a producer on ingestor "missing" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" because "ingestor not found"
    And <session> cannot open a producer on ingestor "orders_http" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" because "not a client ingestor"
    And <session> cannot open a producer on ingestor "orders_in" expecting fields "region STRING, order_id STRING" because "schema mismatch"
    And <session> cannot open a producer on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" with 1 batch and "33MiB" of credit because "invalid limits"
    When <session> opens producer "large" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" with 1 batch and "32MiB" of credit
    Then <session> cannot open a producer on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" because "session capacity exhausted"
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Then producer "large" eventually ends because "domain stopped"
    And <session> cannot open a producer on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" because "domain stopped"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    And within "30s" <session> opens producer "restarted" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "restarted" submits batch "after start" with rows
      | region | order_id | amount | card   |
      | us     | o-9      | 4      | 4111-9 |
    Then batch "after start" completes
    When these NSPL commands are executed on the leader node
      """
      DROP INGESTOR orders_in;
      """
    Then producer "restarted" eventually ends because "endpoint removed"

    Examples:
      | session                 | cluster_size |
      | client "app"            | 3            |
      | WebSocket session "app" | 1            |

  @client_ingestor
  Scenario Outline: Producers of one ingestor share its one acknowledgement window
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And client "first" is connected to node "node-1"
    And client "second" is connected to node "node-1"
    And client "third" is connected to node "node-1"
    When client "first" opens producer "one" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And client "second" opens producer "two" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And client "third" opens producer "three" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "one" submits batch "b1" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    And producer "two" submits batch "b2" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    And producer "three" submits batch "b3" with rows
      | region | order_id | amount | card   |
      | eu     | o-3      | 3      | 4111-3 |
    Then HTTP receiver "sink" eventually receives at least 1 request
    # Three producers hold three batches, and only the declared window of two is ever admitted.
    And within "30s" the leader node describes ingestor "orders_in" with
      """
      producers: 3
      outstanding batches: 3
      admitted batches: 2
      """
    When HTTP receiver "sink" releases its held responses with "respond 200"
    Then batch "b1" completes
    And batch "b2" completes
    And batch "b3" completes
    And HTTP receiver "sink" has captured exactly 3 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_ingestor
  Scenario Outline: A saturated producer keeps its session's commands and clock moving, and a cancelled wait loses nothing
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    And client "app" is connected to node "node-1"
    When client "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" with 2 batches and "1MiB" of credit
    And client "app" executes these NSPL commands
      """
      ATTACH DOMAIN CLOCK;
      """
    And producer "orders" submits batch "b1" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    And producer "orders" submits batch "b2" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    And producer "orders" submits batch "b3" with rows
      | region | order_id | amount | card   |
      | eu     | o-3      | 3      | 4111-3 |
    Then HTTP receiver "sink" eventually receives at least 1 request
    And producer "orders" holds batches "b1, b2"
    And within "30s" the leader node describes ingestor "orders_in" with
      """
      outstanding batches: 2
      admitted batches: 1
      """
    # The producer's credit is spent and its batches wait for the graph, while the same session
    # still answers commands and delivers its attached domain clock.
    And within "10s" client "app" receives a tick for its attached domain clock
    When client "app" executes these NSPL commands
      """
      SHOW INGESTORS;
      """
    Then the last command output contains
      """
      orders_in source=CLIENT schema=order_in
      """
    # Cancelling the wait for credit sends nothing; cancelling the wait for an outcome keeps the
    # batch with the producer.
    When the wait for batch "b3" is cancelled
    And the wait for batch "b1" is cancelled
    Then producer "orders" holds batches "b1, b2"
    When HTTP receiver "sink" releases its held responses with "respond 200"
    Then batch "b2" completes
    When producer "orders" rejoins batch "b1"
    Then batch "b1" completes
    And producer "orders" holds no batches
    And HTTP receiver "sink" has captured exactly 2 requests
    When client "app" executes these NSPL commands
      """
      BEGIN;
      """
    Then client "app" cannot open a producer on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" because "in transaction"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_ingestor
  Scenario Outline: An alteration suspends producers for its hold and keeps them while their endpoint contract holds
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers with
      """
      hold response until released
      """
    And HTTP receiver "sink" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to node "node-1"
    When <session> opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "orders" submits batch "held" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    Then HTTP receiver "sink" eventually receives at least 1 request
    # A new flush cadence keeps the endpoint contract. The alteration's drain waits for the held
    # batch, and admission stays suspended until the alteration commits.
    When these NSPL commands begin executing in the background
      """
      ALTER INGESTOR orders_in REPLACE ROUTE TO orders
        INHERIT ALL UNBRANCHED FLUSH EACH 10ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """
    Then producer "orders" eventually reports admission "suspended"
    When HTTP receiver "sink" releases its held responses with "respond 200"
    Then the background NSPL execution succeeds
    And batch "held" completes
    And producer "orders" eventually reports admission "open"
    When producer "orders" submits batch "after flush change" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    Then batch "after flush change" completes
    # A new acknowledgement window is a new endpoint contract.
    When these NSPL commands are executed on the leader node
      """
      ALTER INGESTOR orders_in SET FROM CLIENT SCHEMA order_in
        MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND;
      """
    Then producer "orders" eventually ends because "endpoint changed"
    When within "30s" <session> opens producer "reopened" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "reopened" submits batch "after contract change" with rows
      | region | order_id | amount | card   |
      | eu     | o-3      | 3      | 4111-3 |
    Then batch "after contract change" completes
    And HTTP receiver "sink" has captured exactly 3 requests

    Examples:
      | session                 | cluster_size |
      | client "app"            | 1            |
      | WebSocket session "app" | 3            |

  @client_ingestor
  Scenario Outline: An alteration whose drain fails keeps every producer attached to the previous execution
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers with
      """
      hold response until released
      """
    And HTTP receiver "sink" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to node "node-1"
    When <session> opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "orders" submits batch "held" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    Then HTTP receiver "sink" eventually receives at least 1 request
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When these NSPL commands fail with "timed out draining domain"
      """
      ALTER INGESTOR orders_in SET FROM CLIENT SCHEMA order_in
        MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE INGESTOR orders_in;
      """
    Then the last command output contains
      """
      MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s
      """
    And producer "orders" eventually reports admission "open"
    When producer "orders" submits batch "after rollback" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    And HTTP receiver "sink" releases its held responses with "respond 200"
    Then batch "held" completes
    And batch "after rollback" completes
    And HTTP receiver "sink" has captured exactly 2 requests

    Examples:
      | session                 | cluster_size |
      | client "app"            | 3            |
      | WebSocket session "app" | 1            |

  @client_ingestor @client_ingestor_placement
  Scenario Outline: A producer entering through another node is forwarded to the owner, and a planned relocation ends it
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    # Only the leader serves a console session, so the producer enters through the leader while
    # the ingestor executes on another node.
    Then the current leader node is saved as placeholder "entry"
    And a node other than placeholder "entry" is saved as placeholder "owner"
    When these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{owner}} IGNORE PREFERENCES;
      """
    Then within "30s" node "{{entry}}" eventually reports scheduled "ingestor" "orders_in" owner equals placeholder "owner"
    Given <session> is connected to node "{{entry}}"
    When within "30s" <session> opens producer "forwarded" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "forwarded" submits batch "through entry" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    Then batch "through entry" completes
    And within "30s" the leader node describes ingestor "orders_in" with
      """
      owner: {{owner}}
      producers: 1
      forwarded producers: 1
      """
    When these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{entry}} IGNORE PREFERENCES;
      """
    Then producer "forwarded" eventually ends because "relocated"
    When within "30s" <session> opens producer "local" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "local" submits batch "on the new owner" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    Then batch "on the new owner" completes
    And within "30s" the leader node describes ingestor "orders_in" with
      """
      owner: {{entry}}
      producers: 1
      forwarded producers: 0
      """

    Examples:
      | session                 |
      | client "app"            |
      | WebSocket session "app" |

  @client_ingestor @client_ingestor_placement
  Scenario: A forwarded producer keeps working across a leadership change
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE INGESTOR orders_in;
      """
    Then the last command output owner is saved as placeholder "owner"
    And the current leader node is saved as placeholder "leader"
    And a node other than placeholders "owner" and "leader" is saved as placeholder "entry"
    Given client "app" is connected to node "{{entry}}"
    When client "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "orders" submits batch "before" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    Then batch "before" completes
    When leadership is transferred to node "{{entry}}"
    Then node "{{owner}}" eventually reports leader "{{entry}}"
    When producer "orders" submits batch "after" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    Then batch "after" completes
    And HTTP receiver "sink" has captured exactly 2 requests

  @client_ingestor @client_ingestor_placement
  Scenario: Stopping the node that executes an ingestor ends its forwarded producers and leaves their unresolved batches unknown
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    And a node other than placeholders "leader" and "owner" is saved as placeholder "entry"
    When these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{owner}} IGNORE PREFERENCES;
      """
    Then within "30s" node "{{leader}}" eventually reports scheduled "ingestor" "orders_in" owner equals placeholder "owner"
    Given client "app" is connected to node "{{entry}}"
    When client "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "orders" submits batch "in flight" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    Then HTTP receiver "sink" eventually receives at least 1 request
    When node "{{owner}}" is stopped
    Then producer "orders" eventually ends because "shutting down"
    And batch "in flight" has an unknown outcome because "interrupted"
    When HTTP receiver "sink" releases its held responses with "respond 200"
    And within "60s" client "app" opens producer "failed over" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "failed over" submits batch "after failover" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    Then batch "after failover" completes

  @client_ingestor @client_ingestor_placement
  Scenario: Losing the node that forwards a producer leaves its batches unknown to the client and lets admitted work finish
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    And a node other than placeholders "leader" and "owner" is saved as placeholder "entry"
    When these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{owner}} IGNORE PREFERENCES;
      RELOCATE RELAY orders ONTO NODE {{owner}} IGNORE PREFERENCES;
      RELOCATE EMITTER published ONTO NODE {{owner}} IGNORE PREFERENCES;
      """
    Then within "30s" node "{{leader}}" eventually reports scheduled "ingestor" "orders_in" owner equals placeholder "owner"
    Given client "app" is connected to node "{{entry}}"
    When client "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "orders" submits batch "in flight" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    Then HTTP receiver "sink" eventually receives at least 1 request
    And within "30s" the leader node describes ingestor "orders_in" with
      """
      forwarded producers: 1
      admitted batches: 1
      """
    When node "{{entry}}" is stopped
    Then producer "orders" eventually ends because "session lost"
    And batch "in flight" has an unknown outcome because "session_lost"
    And within "30s" the leader node describes ingestor "orders_in" with
      """
      producers: 0
      admitted batches: 1
      """
    # The detached batch was admitted, so it still reaches its sink and leaves the window.
    When HTTP receiver "sink" releases its held responses with "respond 200"
    Then within "30s" the leader node describes ingestor "orders_in" with
      """
      producers: 0
      admitted batches: 0
      """
    And HTTP receiver "sink" has captured exactly 1 request

  @client_ingestor
  Scenario Outline: A producer ends with its session across a full restart and a new one publishes again
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to node "node-1"
    When client "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "orders" submits batch "before restart" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    Then batch "before restart" completes
    When the cluster is restarted
    Then producer "orders" eventually ends because "session lost"
    When within "60s" client "app" opens producer "restarted" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "restarted" submits batch "after restart" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    Then batch "after restart" completes
    And HTTP receiver "sink" has captured exactly 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_ingestor
  Scenario Outline: A batch beyond a producer's credit is refused without graph effect and ends the producer
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (
        region STRING, order_id STRING, amount I64, card STRING SENSITIVE
      );
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And WebSocket session "app" is connected to node "node-1"
    When WebSocket session "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE" with 1 batch and "1MiB" of credit
    And producer "orders" submits batch "within" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 1      | 4111-1 |
    And producer "orders" submits batch "beyond" with rows
      | region | order_id | amount | card   |
      | us     | o-2      | 2      | 4111-2 |
    Then batch "beyond" is not admitted because "credit exceeded"
    When HTTP receiver "sink" releases its held responses with "respond 200"
    Then batch "within" completes
    And producer "orders" eventually ends because "protocol violated"
    And HTTP receiver "sink" has captured exactly 1 request
    And HTTP receiver "sink" request 1 is
      """
      POST /orders/eu/o-1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

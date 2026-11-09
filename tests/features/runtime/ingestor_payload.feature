Feature: Ingestor payload facts
  Payload facts describe the exact input handed to the decoder, independently of transport
  headers and keys. Client producers also expose the row count of their admitted Arrow batch.

  Scenario Outline: HTTP payload bytes drive the arrival filter and route while peer IP excludes its port
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA measured (seq I64, bytes U64, remote_ip STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA measured UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-payload.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      """
    When these Plexus statements are executed
      """
      CREATE HTTP ENDPOINT INGESTOR measured_in
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        FILTER WHERE payload.bytes > (9 AS U64)
        TO events INHERIT ALL SET bytes = payload.bytes, remote_ip = metadata.remote_ip
          WHERE payload.bytes < (11 AS U64)
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    And HTTP event 1 of 9 bytes is posted to node "<intake_node>" host "http-{{test_id}}-payload.example.com" path "/events"
    And HTTP event 22 of 10 bytes is posted to node "<intake_node>" host "http-{{test_id}}-payload.example.com" path "/events"
    And HTTP event 333 of 11 bytes is posted to node "<intake_node>" host "http-{{test_id}}-payload.example.com" path "/events"
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":22 | "bytes":10 | "remote_ip":"127.0.0.1"
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-3      |

  Scenario Outline: Kafka payload bytes exclude the record key and preserve filtered delivery
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka metadata topic "payload_{{test_id}}" is created
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA measured (seq I64, bytes U64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA measured UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG {'bootstrap.servers' = '{{kafka_addr}}'};
      CREATE KAFKA TOPIC INGESTOR measured_in FROM broker TOPIC 'payload_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'payload_{{test_id}}' START AT EARLIEST MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING event_codec
        FILTER WHERE payload.bytes > (9 AS U64)
        TO events INHERIT ALL SET bytes = payload.bytes
          WHERE payload.bytes < (11 AS U64)
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    And these Kafka records are published to topic "payload_{{test_id}}"
      """
      [
        {"key":[1,2,3,4,5],"payload":"{\"seq\":1}","timestamp":"2025-01-01T00:00:00Z"},
        {"key":[1,2,3,4,5],"payload":"{\"seq\":22}","timestamp":"2025-01-01T00:00:00Z"},
        {"key":[1,2,3,4,5],"payload":"{\"seq\":333}","timestamp":"2025-01-01T00:00:00Z"}
      ]
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":22 | "bytes":10
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client producer projects its whole batch row count and positive Arrow byte count
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (region STRING, order_id STRING, amount I64, card STRING SENSITIVE);
      CREATE SCHEMA measured (region STRING, order_id STRING, amount I64, card STRING SENSITIVE, bytes U64, rows U64);
      CREATE RELAY events SCHEMA measured UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR measured_in FROM SCHEMA order_in
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        FILTER WHERE payload.rows = (2 AS U64) AND payload.bytes > (0 AS U64)
        TO events INHERIT ALL SET bytes = payload.bytes, rows = payload.rows
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens producer "orders" on ingestor "measured_in" expecting fields "region STRING, order_id STRING, amount I64, card STRING SENSITIVE"
    And producer "orders" submits batch "measured" with rows
      | region | order_id | amount | card   |
      | eu     | o-1      | 12     | 4111-1 |
      | us     | o-2      | 7      | 4111-2 |
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "order_id":"o-1" | "rows":2 | "bytes":
      "order_id":"o-2" | "rows":2 | "bytes":
      """
    And the last relay subscription field "bytes" equals the IPC byte count of batch "measured"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The arrival filter admits only payloads larger than 1024 bytes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA measured (seq I64, bytes U64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA measured UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-threshold.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR measured_in
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        FILTER WHERE payload.bytes > (1024 AS U64)
        TO events INHERIT ALL SET bytes = payload.bytes
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    When HTTP event 1 of 1023 bytes is posted to node "<intake_node>" host "http-{{test_id}}-threshold.example.com" path "/events"
    And HTTP event 2 of 1024 bytes is posted to node "<intake_node>" host "http-{{test_id}}-threshold.example.com" path "/events"
    And HTTP event 3 of 1025 bytes is posted to node "<intake_node>" host "http-{{test_id}}-threshold.example.com" path "/events"
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":3 | "bytes":1025
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-3      |

Feature: JAQ codec unfolding
  Scenario Outline: An HTTP endpoint ingestor unfolds a JSON array into one message per element
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA order_line (
        order_id STRING,
        sku STRING,
        quantity I64
      );
      CREATE CODEC order_lines_codec
        FROM JSON
        TO SCHEMA order_line
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY order_lines SCHEMA order_line UNBRANCHED;
      CREATE VHOST edge unfold-{{test_id}}.example.com;
      CREATE ENDPOINT order_lines_endpoint ON edge PATH '/order-lines' TYPE HTTP;
      CREATE INGESTOR order_lines_source
        FROM ENDPOINT order_lines_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING order_lines_codec
        TO order_lines
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION order_lines_subscription TO order_lines;
      START;
      """
    And http payload is posted to host "unfold-{{test_id}}.example.com" path "/order-lines"
      """
      [{"order_id":"o-1","sku":"apple","quantity":3},{"order_id":"o-1","sku":"pear","quantity":1},{"order_id":"o-1","sku":"plum","quantity":7}]
      """
    Then within "10s" the relay subscription receives payloads in order
      """
      "sku":"apple"
      "sku":"pear"
      "sku":"plum"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An HTTP endpoint ingestor unfolds newline-delimited JSON into one message per line
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA order_line (
        order_id STRING,
        sku STRING,
        quantity I64
      );
      CREATE CODEC order_line_stream_codec
        FROM JSON
        TO SCHEMA order_line
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY order_lines SCHEMA order_line UNBRANCHED;
      CREATE VHOST edge unfold-{{test_id}}.example.com;
      CREATE ENDPOINT order_lines_endpoint ON edge PATH '/order-lines' TYPE HTTP;
      CREATE INGESTOR order_lines_source
        FROM ENDPOINT order_lines_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING order_line_stream_codec
        TO order_lines
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION order_lines_subscription TO order_lines;
      START;
      """
    And http payload is posted to host "unfold-{{test_id}}.example.com" path "/order-lines"
      """
      {"order_id":"o-2","sku":"fig","quantity":2}
      {"order_id":"o-2","sku":"kiwi","quantity":5}
      {"order_id":"o-2","sku":"lime","quantity":9}
      """
    Then within "10s" the relay subscription receives payloads in order
      """
      "sku":"fig"
      "sku":"kiwi"
      "sku":"lime"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A payload whose unfolded element does not fit the schema is rejected as a whole
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA event (
        user_id I64
      );
      CREATE CODEC events_codec
        FROM JSON
        TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge unfold-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR events_source
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING events_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    And http payload is posted to host "unfold-{{test_id}}.example.com" path "/events"
      """
      [{"user_id":1},{"user_id":"secret-two"},{"user_id":3}]
      """
    Then within "10s" the active session observes a server error
    And the last server error contains
      """
      codec 'events_codec' failed to parse field 'user_id'
      """
    And the last server error contains
      """
      (input value 0, output 1)
      """
    And the last server error does not contain
      """
      secret-two
      """
    When http payload is posted to host "unfold-{{test_id}}.example.com" path "/events"
      """
      [{"user_id":4}]
      """
    Then within "10s" the relay subscription receives payloads in order
      """
      "user_id":4
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A payload that unfolds beyond the limit is rejected and the ingestor keeps serving
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA event (
        user_id I64
      );
      CREATE CODEC flood_codec
        FROM JSON
        TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION 'if .flood then range(65537) | {user_id: .} else . end';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge unfold-{{test_id}}.example.com;
      CREATE ENDPOINT flood_endpoint ON edge PATH '/flood' TYPE HTTP;
      CREATE INGESTOR flood_source
        FROM ENDPOINT flood_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING flood_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    And http payload is posted to host "unfold-{{test_id}}.example.com" path "/flood"
      """
      {"flood":true}
      """
    Then within "20s" the active session observes a server error
    And the last server error contains
      """
      codec 'flood_codec' payload exceeds the unfold limit of 65536 messages
      """
    When http payload is posted to host "unfold-{{test_id}}.example.com" path "/flood"
      """
      {"user_id":7}
      """
    Then within "10s" the relay subscription receives payloads in order
      """
      "user_id":7
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Kafka payload that unfolds into no messages is acknowledged
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "selected_events_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA event (
        user_id I64
      );
      CREATE CODEC selected_events_codec
        FROM JSON
        TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[] | select(.keep)';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE INGESTOR selected_event_source
        FROM KAFKA kafka_main TOPIC selected_events_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_selected_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING selected_events_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    # ACK SEQUENTIAL takes the second payload only once the first has been acknowledged, and an
    # unresolved acknowledgement would hold it for the whole 30s ACK TIMEOUT.
    And Kafka message is published to topic "selected_events_{{test_id}}" partition 0
      """
      [{"user_id":1,"keep":false},{"user_id":2,"keep":false}]
      """
    And Kafka message is published to topic "selected_events_{{test_id}}" partition 0
      """
      [{"user_id":3,"keep":true}]
      """
    Then within "20s" the relay subscription receives payloads in order
      """
      "user_id":3
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Kafka ACK SEQUENTIAL payload is committed only after every message it unfolds into is acknowledged
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "split_events_{{test_id}}" exists with 1 partitions
    And Kafka topic "held_events_out_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA split_event (
        user_id I64,
        target STRING
      );
      CREATE WIRE JSON SCHEMA split_event_wire MODE STRICT (
        user_id integer,
        target string
      );
      CREATE CODEC split_event_wire_codec
        FROM WIRE JSON SCHEMA split_event_wire
        TO SCHEMA split_event;
      CREATE CODEC split_events_codec
        FROM JSON
        TO SCHEMA split_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY delivered_events SCHEMA split_event UNBRANCHED;
      CREATE RELAY held_events SCHEMA split_event UNBRANCHED;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE INGESTOR split_event_source
        FROM KAFKA kafka_main TOPIC split_events_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_split_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING split_events_codec
        TO delivered_events
          INHERIT ALL
          WHERE message.target = 'delivered'
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        TO held_events
          INHERIT ALL
          WHERE message.target = 'held'
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER held_events_forward FROM held_events TO KAFKA kafka_main TOPIC held_events_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING split_event_wire_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION delivered_events_subscription TO delivered_events;
      START;
      """
    And emitter "held_events_forward" enters stall mode
    And Kafka message is published to topic "split_events_{{test_id}}" partition 0
      """
      [{"user_id":1,"target":"delivered"},{"user_id":2,"target":"held"}]
      """
    Then within "20s" the relay subscription receives payloads in order
      """
      "user_id":1
      """
    When Kafka message is published to topic "split_events_{{test_id}}" partition 0
      """
      [{"user_id":3,"target":"delivered"}]
      """
    # The stalled emitter holds the first payload's second message for as long as the stall lasts,
    # so the second payload must not be taken at all before the stall is released.
    Then the relay subscription does not receive a payload within "2s"
    When emitter "held_events_forward" leaves stall mode
    Then within "20s" the relay subscription receives payloads in order
      """
      "user_id":3
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A negative acknowledgement of one unfolded message redelivers its whole Kafka payload
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "split_events_{{test_id}}" exists with 1 partitions
    And Kafka topic "held_events_out_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA split_event (
        user_id I64,
        target STRING
      );
      CREATE WIRE JSON SCHEMA split_event_wire MODE STRICT (
        user_id integer,
        target string
      );
      CREATE CODEC split_event_wire_codec
        FROM WIRE JSON SCHEMA split_event_wire
        TO SCHEMA split_event;
      CREATE CODEC split_events_codec
        FROM JSON
        TO SCHEMA split_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY delivered_events SCHEMA split_event UNBRANCHED;
      CREATE RELAY held_events SCHEMA split_event UNBRANCHED;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE INGESTOR split_event_source
        FROM KAFKA kafka_main TOPIC split_events_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_split_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING split_events_codec
        TO delivered_events
          INHERIT ALL
          WHERE message.target = 'delivered'
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        TO held_events
          INHERIT ALL
          WHERE message.target = 'held'
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER held_events_forward FROM held_events TO KAFKA kafka_main TOPIC held_events_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING split_event_wire_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION delivered_events_subscription TO delivered_events;
      START;
      """
    And emitter "held_events_forward" enters fault mode
    And Kafka message is published to topic "split_events_{{test_id}}" partition 0
      """
      [{"user_id":1,"target":"delivered"},{"user_id":2,"target":"held"}]
      """
    Then within "20s" the relay subscription receives payloads in order
      """
      "user_id":1
      """
    # The faulted emitter negatively acknowledges the second message, so the whole payload is
    # replayed and its already acknowledged first message is delivered again.
    And within "20s" the relay subscription receives payloads in order
      """
      "user_id":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Messages unfolded from one Kafka payload reach different branches with the same source metadata
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "tenant_batches_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA tenant_event (
        tenant STRING,
        user_id I64
      );
      CREATE SCHEMA sourced_tenant_event (
        tenant STRING,
        user_id I64,
        source_partition I32 OPTIONAL,
        source_offset I64 OPTIONAL,
        route STRING
      );
      CREATE CODEC tenant_batch_codec
        FROM JSON
        TO SCHEMA tenant_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.events[]';
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY tenant_events SCHEMA sourced_tenant_event BRANCHED BY by_tenant;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE INGESTOR tenant_batch_source
        FROM KAFKA kafka_main TOPIC tenant_batches_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_tenant_batches_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING tenant_batch_codec
        TO tenant_events
        INHERIT ALL
        SET source_partition = metadata.partition,
            source_offset = metadata.offset,
            route = coalesce(read_header('route'), 'absent')
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION tenant_events_subscription TO tenant_events;
      START;
      """
    And Kafka message with headers "route=primary" is published to topic "tenant_batches_{{test_id}}" partition 0
      """
      {"events":[{"tenant":"acme","user_id":1},{"tenant":"beta","user_id":2}]}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "user_id":1 | "source_partition":0 | "source_offset":0 | "route":"primary"
      key={"tenant":"beta"} | "user_id":2 | "source_partition":0 | "source_offset":0 | "route":"primary"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An HTTP endpoint ingestor unfolds every value of a <format> payload
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
      CREATE CODEC notification_codec
        FROM <format>
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '<transformation>';
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge unfold-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE INGESTOR notifications_source
        FROM ENDPOINT notifications_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And JAQ native payload fixture "<payload_fixture>" is posted to host "unfold-{{test_id}}.example.com" path "/notifications"
    Then within "10s" the relay subscription receives payloads in order
      """
      "payload":"first"
      "payload":"second"
      """

    Examples:
      | cluster_size | format | payload_fixture                 | transformation                                                                                                         |
      | 1            | YAML   | yaml_notification_stream        | .                                                                                                                      |
      | 3            | YAML   | yaml_notification_stream        | .                                                                                                                      |
      | 1            | CBOR   | cbor_notification_sequence      | .                                                                                                                      |
      | 3            | CBOR   | cbor_notification_sequence      | .                                                                                                                      |
      | 1            | XML    | xml_declared_notification_batch | .c[] \| {user_id: (.c[] \| select(.t == "user_id").c[0] \| tonumber), payload: (.c[] \| select(.t == "payload").c[0])} |
      | 3            | XML    | xml_declared_notification_batch | .c[] \| {user_id: (.c[] \| select(.t == "user_id").c[0] \| tonumber), payload: (.c[] \| select(.t == "payload").c[0])} |

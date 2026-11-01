Feature: RabbitMQ exchange emission and unroutable messages
  A RABBITMQ EXCHANGE emitter publishes to an exchange the operator declared, routing each record by
  the key its ROUTING KEY expression computes for it, and a RABBITMQ QUEUE emitter publishes through
  the default exchange with its queue's name as the key. An exchange that does not exist leaves the
  emitter unopened until it is declared. Written MANDATORY, a message the broker cannot route comes
  back to the emitter, which rejects its record through ON MESSAGE ERROR in every mode; without it
  the broker drops the message and the emitter reports it delivered.

  Scenario Outline: A RabbitMQ exchange emitter routes every record by the key it computes
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ exchange "events_{{test_id}}" of type <exchange_type> exists
    And RabbitMQ queue "routed_{{test_id}}" is bound to exchange "events_{{test_id}}" with routing key "<binding>"
    And RabbitMQ queue "routed_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, category STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, category string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge exchange-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ EXCHANGE EMITTER routed FROM events
        TO broker EXCHANGE 'events_{{test_id}}' ROUTING KEY input.category
          MODE <mode> RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "exchange-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"category":"order.created"},{"seq":2,"category":"audit.login"},{"seq":3,"category":"order.paid"}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      <first>
      <second>
      <third>
      """
    And SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter | clause                                       |
      | routed  | ROUTING KEY input.category                   |
      | routed  | MODE <mode> RETRY POLICY BACKOFF 50ms MAX 1s |

    # A direct exchange routes the key its binding names exactly, a topic exchange every key its
    # binding pattern matches, and a fanout exchange every message whatever its key. An empty cell
    # is a payload the queue does not receive.
    Examples:
      | cluster_size | exchange_type | binding       | mode                               | first                                | second                             | third                             |
      | 1            | direct        | order.created | NO ACK                             | {"seq":1,"category":"order.created"} |                                    |                                   |
      | 3            | direct        | order.created | ACK SEQUENTIAL ACK TIMEOUT 30s     | {"seq":1,"category":"order.created"} |                                    |                                   |
      | 1            | topic         | order.*       | ACK PARALLEL MAX 2 ACK TIMEOUT 30s | {"seq":1,"category":"order.created"} |                                    | {"seq":3,"category":"order.paid"} |
      | 3            | topic         | order.*       | NO ACK                             | {"seq":1,"category":"order.created"} |                                    | {"seq":3,"category":"order.paid"} |
      | 1            | fanout        | ignored       | ACK SEQUENTIAL ACK TIMEOUT 30s     | {"seq":1,"category":"order.created"} | {"seq":2,"category":"audit.login"} | {"seq":3,"category":"order.paid"} |
      | 3            | fanout        | ignored       | ACK PARALLEL MAX 2 ACK TIMEOUT 30s | {"seq":1,"category":"order.created"} | {"seq":2,"category":"audit.login"} | {"seq":3,"category":"order.paid"} |

  Scenario Outline: A RabbitMQ exchange batch message carries the routing key its members share
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ exchange "events_{{test_id}}" of type direct exists
    And RabbitMQ queue "routed_{{test_id}}" is bound to exchange "events_{{test_id}}" with routing key "order.created"
    And RabbitMQ queue "routed_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, category STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, category string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge batched-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ EXCHANGE EMITTER routed FROM events
        TO broker EXCHANGE 'events_{{test_id}}' ROUTING KEY input.category
          MODE <mode> RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 3 MAX SIZE 64KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "batched-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"category":"order.created"},{"seq":2,"category":"order.created"},{"seq":3,"category":"audit.login"}]
      """
    # The two records under one key leave in one message, which the exchange routes by that key;
    # the third record's key seals the batch and the exchange routes its message elsewhere.
    Then within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":1,"category":"order.created"},{"seq":2,"category":"order.created"}]
      """

    Examples:
      | cluster_size | mode                               |
      | 1            | NO ACK                             |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 30s |

  @rabbitmq_exchange_lifecycle @deloxide_stress
  Scenario Outline: A RabbitMQ exchange emitter opens once its missing exchange is declared
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, category STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, category string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge provisioned-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ EXCHANGE EMITTER routed FROM events
        TO broker EXCHANGE 'provisioned_{{test_id}}' ROUTING KEY input.category
          MODE <mode> RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "routed" on the leader node contains
      """
      transient error: RabbitMQ exchange 'provisioned_{{test_id}}' does not exist
      """
    When the operator provisions RabbitMQ exchange "provisioned_{{test_id}}" of type direct
    And the operator binds RabbitMQ queue "provisioned_{{test_id}}" to exchange "provisioned_{{test_id}}" with routing key "order.created"
    And the operator recreates RabbitMQ queue "provisioned_{{test_id}}" and observes it
    And http payload is posted to host "provisioned-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"category":"order.created"}]
      """
    Then within "60s" the observed broker receives exactly these payloads
      """
      {"seq":1,"category":"order.created"}
      """

    Examples:
      | cluster_size | mode                           |
      | 1            | NO ACK                         |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 30s |

  Scenario Outline: MANDATORY rejects a record its exchange cannot route in every mode
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ exchange "events_{{test_id}}" of type direct exists
    And RabbitMQ queue "routed_{{test_id}}" is bound to exchange "events_{{test_id}}" with routing key "order.created"
    And RabbitMQ queue "routed_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, category STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, category string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge mandatory-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ EXCHANGE EMITTER routed FROM events
        TO broker EXCHANGE 'events_{{test_id}}' ROUTING KEY input.category
          MODE <mode> RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
          MANDATORY
        INHERIT ALL
        <batch>
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "mandatory-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"category":"order.created"},{"seq":2,"category":"order.unrouted"},{"seq":3,"category":"order.created"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":2 | "error_code":"external" | "operation":"publish" | reply code 312 | NO_ROUTE
      """
    And within "30s" the observed broker receives exactly these payloads
      """
      <first>
      <third>
      """

    # Records whose keys differ never share a batch message, so where the emitter batches, each
    # record leaves in a message of its own and the unroutable one is rejected alone.
    Examples:
      | cluster_size | mode                               | batch                               | first                                  | third                                  |
      | 1            | NO ACK                             |                                     | {"seq":1,"category":"order.created"}   | {"seq":3,"category":"order.created"}   |
      | 3            | NO ACK                             | BATCH MAX MESSAGES 3 MAX SIZE 64KiB | [{"seq":1,"category":"order.created"}] | [{"seq":3,"category":"order.created"}] |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 30s     |                                     | {"seq":1,"category":"order.created"}   | {"seq":3,"category":"order.created"}   |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 30s     |                                     | {"seq":1,"category":"order.created"}   | {"seq":3,"category":"order.created"}   |
      | 1            | ACK PARALLEL MAX 2 ACK TIMEOUT 30s | BATCH MAX MESSAGES 3 MAX SIZE 64KiB | [{"seq":1,"category":"order.created"}] | [{"seq":3,"category":"order.created"}] |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 30s |                                     | {"seq":1,"category":"order.created"}   | {"seq":3,"category":"order.created"}   |

  Scenario Outline: MANDATORY rejects a record its deleted queue can no longer receive
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "deleted_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, category STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, category string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge deleted-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ QUEUE EMITTER queued FROM events
        TO broker QUEUE 'deleted_{{test_id}}'
          MODE <mode> RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
          MANDATORY
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "deleted-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"category":"order.created"}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"seq":1,"category":"order.created"}
      """
    When RabbitMQ queue "deleted_{{test_id}}" is deleted
    And http payload is posted to host "deleted-{{test_id}}.example.com" path "/events"
      """
      [{"seq":2,"category":"order.created"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":2 | "error_code":"external" | "operation":"publish" | reply code 312 | NO_ROUTE
      """

    Examples:
      | cluster_size | mode                               |
      | 1            | NO ACK                             |
      | 3            | NO ACK                             |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 30s     |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 30s |

  Scenario Outline: Without MANDATORY the broker drops a message its deleted queue can no longer receive
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "dropped_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, category STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, category string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge dropped-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ QUEUE EMITTER queued FROM events
        TO broker QUEUE 'dropped_{{test_id}}'
          MODE <mode> RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "dropped-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"category":"order.created"}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"seq":1,"category":"order.created"}
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER queued;
      """
    Then the last command output owner is saved as placeholder "queued_owner"
    When RabbitMQ queue "dropped_{{test_id}}" is deleted
    And http payload is posted to host "dropped-{{test_id}}.example.com" path "/events"
      """
      [{"seq":2,"category":"order.created"}]
      """
    # The broker answers the message it cannot route as taken, so the emitter counts it sent and
    # never writes it again, even once the queue exists again.
    Then within "30s" DESCRIBE EMITTER "queued" on the leader node contains
      """
      messages_total sent relay=events physical_node={{queued_owner}} total=2
      """
    When the operator recreates RabbitMQ queue "dropped_{{test_id}}" and observes it
    And http payload is posted to host "dropped-{{test_id}}.example.com" path "/events"
      """
      [{"seq":3,"category":"order.created"}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"seq":3,"category":"order.created"}
      """

    Examples:
      | cluster_size | mode                               |
      | 1            | NO ACK                             |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 30s     |
      | 1            | ACK PARALLEL MAX 2 ACK TIMEOUT 30s |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 30s |

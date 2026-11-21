Feature: Bounded execution and transient memory

  @execution_class_drain @deloxide_stress
  Scenario Outline: Occupied bulk execution leaves management work responsive
    Given a <cluster_size> node nervix cluster is started
    And client "management" is connected to node "<node_id>"
    When bulk execution on every node is occupied
    Then node "<node_id>" observability path "/readyz" eventually responds with 200 and "ready"
    And within "10s" client "management" completes these Plexus statements
      """
      CREATE DOMAIN {{domain}};
      CREATE DOMAIN {{domain}}_first;
      CREATE DOMAIN {{domain}}_second;
      CREATE DOMAIN {{domain}}_third;
      CREATE DOMAIN {{domain}}_fourth;
      CREATE DOMAIN {{domain}}_fifth;
      """
    And the last command output contains
      """
      created domain '{{domain}}_fifth'
      """
    And within "10s" client "management" completes these Plexus statements
      """
      LIST DOMAINS;
      """
    And the last command output contains
      """
      {{domain}}_fifth
      """
    And node "<node_id>" eventually observes a stable leader

    Examples:
      | cluster_size | node_id |
      | 1            | node-1  |

    @slots_12
    Examples:
      | cluster_size | node_id |
      | 3            | node-2  |

  Scenario Outline: Credentials a node cannot verify now are answered as unavailable
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE USER busy_user WITH PASSWORD 'busy-password';
      """
    And credential verification on the leader node is saturated
    And the client connects to the saturated node as user "busy_user" with password "busy-password"
    Then the last command error contains
      """
      the node could not verify credentials now; retry
      """
    When the saturated credential verification is released
    And the client connects to the leader node as user "busy_user" with password "busy-password"
    Then the last command output contains
      """
      raft.current_leader:
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_encoding_waits_for_extension_room
  Scenario Outline: An emitter's transformed encoding waits for the extension workers while no node has room for it
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "transformed_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC notification_in FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE JAQ JSON CODEC notification_out
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON EMITTING '{payload: .}';
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge transformed-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/notifications';
      CREATE HTTP ENDPOINT INGESTOR notification_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING notification_in
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}'
        };
      CREATE KAFKA TOPIC EMITTER transformed_notifications FROM notifications TO kafka_main TOPIC 'transformed_out_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 1s ENCODE USING notification_out
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And extension execution is saturated on every node
    And http payload is posted to host "transformed-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42}
      """
    # While every extension worker and wait-queue place is held, the encoding waits for a place:
    # the emitter keeps its rows, publishes nothing yet and reports no failure.
    Then the observed broker does not receive a payload within "3s"
    And DESCRIBE EMITTER "transformed_notifications" on the leader node reports no transient error for "3s"
    When extension execution is released on every node
    Then the observed broker receives a payload
      """
      {"payload":{"user_id":42}}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @retained_unfolding_waits_for_extension_room
  Scenario Outline: A retained endpoint body waits for the extension workers when it drains
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA retained_event ( seq I64 );
      CREATE JAQ JSON CODEC retained_codec TO SCHEMA retained_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE JAQ JSON CODEC delivery_codec TO SCHEMA retained_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY retained_events SCHEMA retained_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-retained-unfolding.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR retained_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING retained_codec
        TO retained_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION retained_subscription TO retained_events;
      START;
      """
    Given the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands begin executing in the background
      """
      ALTER HTTP ENDPOINT INGESTOR retained_ingestor SET DECODE USING delivery_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "10s" node "node-1" eventually reports describe ingestor "retained_ingestor" as "status: quiesced"
    When http payload is posted to node "node-1" with host "http-{{test_id}}-retained-unfolding.example.com" path "/events"
      """
      {"seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-retained-unfolding.example.com" path "/events"
      """
      {"seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-retained-unfolding.example.com" path "/events"
      """
      {"seq":3}
      """
    Then within "10s" node "node-1" eventually reports describe ingestor "retained_ingestor" as "nervix_ingestor_quiesce_buffered_records: 3"
    When extension execution is saturated on every node
    And the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And within "10s" node "node-1" eventually reports describe ingestor "retained_ingestor" as "status: running"
    And within "10s" node "node-1" eventually reports describe ingestor "retained_ingestor" as "nervix_ingestor_quiesce_buffered_records: 3"
    When extension execution is released on every node
    Then within "30s" the relay subscription receives payloads in order
      """
      "seq":1
      "seq":2
      "seq":3
      """
    And within "10s" node "node-1" eventually reports describe ingestor "retained_ingestor" as "nervix_ingestor_quiesce_buffered_records: 0"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @polled_unfolding_waits_for_extension_room
  Scenario Outline: A polled payload waits for the extension workers when the node cannot unfold it
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA polled_event ( user_id I64 );
      CREATE JAQ JSON CODEC polled_codec TO SCHEMA polled_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY polled_events SCHEMA polled_event UNBRANCHED;
      CREATE HTTP CLIENT polled_http CONFIG {
        'endpoint' = '{{mock_http_addr}}/http/{{test_id}}',
        'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR polled_ingestor
        FROM polled_http METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION polled_subscription TO polled_events;
      """
    When extension execution is saturated on every node
    And these Plexus commands are executed
      """
      START;
      """
    # The mock answers only the first poll with its payload, and nothing can be unfolded while
    # every extension worker and place in the extension wait queue is held.
    Then the HTTP mock eventually answers a poll with its payload
    And the relay subscription does not receive a payload within "3s"
    When extension execution is released on every node
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @unacknowledged_unfolding_waits_for_extension_room
  Scenario Outline: An unacknowledged <source> payload waits for the extension workers when the node cannot unfold it
    Given unacknowledged source "<source>" is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the unacknowledged source "<source>" ingestor is created
    And extension execution is saturated on every node
    And these Plexus commands are executed
      """
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "unacknowledged_source" on the leader node contains
      """
      ready: true
      """
    When the unacknowledged source "<source>" delivers the payload of user 42
    # Nothing can be unfolded while every extension worker and place in the extension wait queue
    # is held, so the ingestor holds the payload and takes nothing more from its source, which
    # keeps the payload sent after it.
    Then the relay subscription does not receive a payload within "3s"
    When the unacknowledged source "<source>" delivers the payload of user 7
    And extension execution is released on every node
    Then within "30s" the relay subscription receives payloads in order
      """
      "user_id":42
      "user_id":7
      """
    And the relay subscription does not receive a payload within "2s"
    And the active session observes no server error containing "did not unfold" within "100ms"

    Examples:
      | cluster_size | source           |
      | 1            | nats             |
      | 3            | nats             |
      | 1            | syslog           |
      | 3            | syslog           |
      | 1            | zeromq           |
      | 3            | zeromq           |
      | 1            | kafka            |
      | 3            | kafka            |
      | 1            | mqtt             |
      | 3            | mqtt             |
      | 1            | pulsar           |
      | 3            | pulsar           |
      | 1            | rabbitmq         |
      | 3            | rabbitmq         |
      | 1            | redis            |
      | 3            | redis            |
      | 1            | websocket client |
      | 3            | websocket client |

  @unacknowledged_endpoint_frame_waits_for_extension_room
  Scenario Outline: An unacknowledged WebSocket endpoint frame waits for the extension workers when the node cannot unfold it
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA frame_event ( seq I64 );
      CREATE JAQ JSON CODEC frame_codec TO SCHEMA frame_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY frame_events SCHEMA frame_event UNBRANCHED;
      CREATE VHOST edge ws-{{test_id}}-frame-unfolding.example.com;
      CREATE WEBSOCKETS ENDPOINT ingress ON edge PATH '/frames';
      CREATE WEBSOCKETS ENDPOINT INGESTOR frame_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING frame_codec
        TO frame_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION frame_subscription TO frame_events;
      START;
      """
    When an endpoint websocket is opened on node "node-1" host "ws-{{test_id}}-frame-unfolding.example.com" path "/frames"
    And extension execution is saturated on every node
    And a payload is sent on the endpoint websocket
      """
      {"seq":1}
      """
    And a payload is sent on the endpoint websocket
      """
      {"seq":2}
      """
    # A frame in MODE NO ACK has no answer its sender waits for, so while every extension worker
    # and place in the extension wait queue is held the first frame waits, and the connection
    # reads nothing further, instead of closing with 1013 and losing the frame.
    Then the endpoint websocket stays open and silent for "3s"
    And the relay subscription does not receive a payload within "1s"
    When extension execution is released on every node
    Then within "30s" the relay subscription receives payloads in order
      """
      "seq":1
      "seq":2
      """
    And the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The ingestor's client connects to the broker listener that holds it to a 10-second keep-alive
  # and closes a connection that sends nothing for 15 seconds. The payload waits for the extension
  # workers for longer than that, and a payload published after the broker would have closed the
  # connection is only received by a connection that stayed open.
  @unacknowledged_unfolding_keeps_mqtt_connected
  Scenario Outline: An MQTT NO ACK ingestor stays connected while its payload waits for the extension workers
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA waiting_event ( user_id I64 );
      CREATE JAQ JSON CODEC waiting_codec TO SCHEMA waiting_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY waiting_events SCHEMA waiting_event UNBRANCHED;
      CREATE MQTT CLIENT waiting_client CONFIG {
        'addr' = '{{mqtt_short_keepalive_addr}}',
        'client_id' = 'nervix-cucumber-unfolding-keepalive-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR waiting_ingestor
        FROM waiting_client TOPIC 'unfolding_keepalive_{{test_id}}' SESSION CLEAN QOS 0
        MODE NO ACK ON QUIESCE DROP DECODE USING waiting_codec
        TO waiting_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION waiting_subscription TO waiting_events;
      """
    And extension execution is saturated on every node
    And these Plexus commands are executed
      """
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "waiting_ingestor" on the leader node contains
      """
      ready: true
      """
    When 1 JSON messages with user id 42 are rapidly published to "MQTT" input "unfolding_keepalive_{{test_id}}"
    And physical time passes for "20s"
    And 1 JSON messages with user id 7 are rapidly published to "MQTT" input "unfolding_keepalive_{{test_id}}"
    And extension execution is released on every node
    Then within "30s" the relay subscription receives payloads in order
      """
      "user_id":42
      "user_id":7
      """
    And the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A consumer-group member that polls nothing for max.poll.interval.ms leaves its group. The
  # payload waits for the extension workers for longer than that while the group keeps its member.
  @unacknowledged_unfolding_keeps_kafka_group
  Scenario Outline: A Kafka NO ACK consumer group keeps its member while its payload waits for the extension workers
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "unfolding_group_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA waiting_event ( user_id I64 );
      CREATE JAQ JSON CODEC waiting_codec TO SCHEMA waiting_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY waiting_events SCHEMA waiting_event UNBRANCHED;
      CREATE KAFKA CLIENT waiting_client CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'session.timeout.ms' = '6000',
        'max.poll.interval.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE KAFKA TOPIC INGESTOR waiting_ingestor
        FROM waiting_client TOPIC 'unfolding_group_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'unfolding_group_{{test_id}}' START AT EARLIEST
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING waiting_codec
        TO waiting_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION waiting_subscription TO waiting_events;
      """
    And extension execution is saturated on every node
    And these Plexus commands are executed
      """
      START;
      """
    Then within "30s" Kafka consumer group "unfolding_group_{{test_id}}" eventually has 1 consumers
    When these Kafka messages are rapidly published to topic "unfolding_group_{{test_id}}"
      """
      {"user_id":42}
      {"user_id":7}
      """
    Then Kafka consumer group "unfolding_group_{{test_id}}" keeps its single stable member for "9s"
    When extension execution is released on every node
    Then within "30s" the relay subscription receives payloads in order
      """
      "user_id":42
      "user_id":7
      """
    And within "30s" Kafka consumer group "unfolding_group_{{test_id}}" next offset for topic "unfolding_group_{{test_id}}" partition 0 is "at least 2"
    And the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A graceful STOP lets the work an ingestor already took finish. A payload waiting for the
  # extension workers is such work, so the stop waits for it rather than cutting it.
  @unacknowledged_unfolding_holds_stop
  Scenario Outline: A graceful STOP waits for a payload that waits for the extension workers
    Given unacknowledged source "kafka" is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the unacknowledged source "kafka" ingestor is created
    And extension execution is saturated on every node
    And these Plexus commands are executed
      """
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "unacknowledged_source" on the leader node contains
      """
      ready: true
      """
    When the unacknowledged source "kafka" delivers the payload of user 42
    Then the relay subscription does not receive a payload within "3s"
    When these Plexus commands begin executing in the background
      """
      STOP;
      """
    Then the background Plexus execution is still running after "3s"
    When extension execution is released on every node
    Then the background Plexus execution succeeds
    And within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @acknowledged_unfolding_waits_for_extension_room
  Scenario Outline: An acknowledged Kafka payload waits for the extension workers instead of being presented again, and counts no decode rejection
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "acknowledged_wait_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA waiting_event ( user_id I64 );
      CREATE JAQ JSON CODEC waiting_codec TO SCHEMA waiting_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY waiting_events SCHEMA waiting_event UNBRANCHED;
      CREATE KAFKA CLIENT waiting_client CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR waiting_ingestor
        FROM waiting_client TOPIC 'acknowledged_wait_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'acknowledged_wait_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING waiting_codec
        TO waiting_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION waiting_subscription TO waiting_events;
      """
    And extension execution is saturated on every node
    And Kafka message is published to topic "acknowledged_wait_{{test_id}}"
      """
      {"user_id":42}
      """
    And these Plexus commands are executed
      """
      START;
      """
    Then the relay subscription does not receive a payload within "3s"
    And the active session observes no server error containing "did not unfold" within "100ms"
    When extension execution is released on every node
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """
    And the relay subscription does not receive a payload within "2s"
    And within "30s" Kafka consumer group "acknowledged_wait_{{test_id}}" next offset for topic "acknowledged_wait_{{test_id}}" partition 0 is "at least 1"
    # A payload that waited for a place on the extension workers was never refused by its codec.
    And within "30s" DESCRIBE INGESTOR "waiting_ingestor" on the leader node contains
      """
      nervix_ingestor_decode_rejections_total: 0
      """
    And within "30s" DESCRIBE INGESTOR "waiting_ingestor" on the leader node contains
      """
      last decode rejection: -
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR waiting_ingestor;
      """
    Then the last command output owner is saved as placeholder "waiting_owner"
    And node "{{waiting_owner}}" observability metric "nervix_ingestor_unfolding_refused_total" with labels eventually equals 0
      """
      ingestor="waiting_ingestor"
      connector="KAFKA"
      entity="TOPIC"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A consumer forwarded from another node is topology-specific: its serving node and the node that
  # owns its emitter must differ, so the scenario runs on three nodes only.
  @client_emitter
  Scenario: A forwarded consumer keeps its stream while its serving node's data workers are full and receives the batch once they free
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR orders_in
        FROM SCHEMA incoming
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER app_output FROM orders
        TO SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "owner"
    And a node other than placeholder "owner" is saved as placeholder "serving"
    When these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{owner}} IGNORE PREFERENCES;
      RELOCATE EMITTER app_output ONTO NODE {{owner}} IGNORE PREFERENCES;
      """
    Then within "30s" node "{{serving}}" eventually reports scheduled "ingestor" "orders_in" owner equals placeholder "owner"
    And within "30s" node "{{serving}}" eventually reports scheduled "emitter" "app_output" owner equals placeholder "owner"
    Given client "app" is connected to node "{{serving}}"
    And client "source" is connected to node "{{owner}}"
    When client "app" opens consumer "remote" on emitter "app_output" expecting fields "id STRING, cents I64"
    Then within "30s" the leader node describes emitter "app_output" with
      """
      consumers: 1
      forwarded consumers: 1
      """
    When client "source" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    # The serving node's data workers carry the forwarded consumer's heartbeats and deliveries.
    # While they are full, those frames wait for a place instead of being refused, so the stream
    # stays open as long as the serving node is released within its heartbeat silence limit.
    And data execution is saturated on node "{{serving}}"
    And producer "input" submits batch "held" with rows
      | id  | amount |
      | o-5 | 7      |
    Then within "30s" the leader node describes emitter "app_output" with
      """
      forwarded retained batches: 1
      """
    When execution is released on node "{{serving}}"
    And consumer "remote" reads output batch "held output"
    Then output batch "held output" contains id "o-5" and cents 700
    When output batch "held output" is acknowledged
    Then batch "held" completes
    And within "30s" the leader node describes emitter "app_output" with
      """
      consumers: 1
      forwarded consumers: 1
      retained batches: 0
      """

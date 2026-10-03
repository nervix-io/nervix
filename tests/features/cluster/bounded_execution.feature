Feature: Bounded execution and transient memory

  Scenario Outline: Occupied bulk execution leaves management work responsive
    Given a <cluster_size> node nervix cluster is started
    When bulk execution on node "<node_id>" is occupied
    Then node "<node_id>" observability path "/readyz" eventually responds with 200 and "ready"
    And within "10s" these NSPL commands complete on node "<node_id>"
      """
      CREATE DOMAIN bounded_execution;
      """
    When bulk execution on node "<node_id>" is released
    Then node "<node_id>" eventually observes a stable leader

    Examples:
      | cluster_size | node_id |
      | 1            | node-1  |
      | 3            | node-2  |

  Scenario Outline: Credentials a node cannot verify now are answered as unavailable
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed through the client on the leader node
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

  Scenario Outline: An emitter keeps its rows while no node can take their transformed encoding
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "transformed_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_in FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE CODEC notification_out
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON EMITTING '{payload: .}';
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge transformed-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/notifications' TYPE HTTP;
      CREATE INGESTOR notification_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_in
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}'
        };
      CREATE EMITTER transformed_notifications FROM notifications TO KAFKA kafka_main TOPIC transformed_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 1s ENCODE USING notification_out
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
    Then within "30s" DESCRIBE EMITTER "transformed_notifications" on the leader node contains
      """
      the node's extension workers have no room now
      """
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA retained_event ( seq I64 );
      CREATE CODEC retained_codec FROM JSON TO SCHEMA retained_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE CODEC delivery_codec FROM JSON TO SCHEMA retained_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY retained_events SCHEMA retained_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-retained-unfolding.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR retained_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING retained_codec
        TO retained_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION retained_subscription TO retained_events;
      START;
      """
    Given the entity gate for domain "{{domain}}" pauses after engagement
    When these NSPL commands begin executing in the background
      """
      ALTER INGESTOR retained_source SET DECODE USING delivery_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "10s" node "node-1" eventually reports describe ingestor "retained_source" as "status: quiesced"
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
    Then within "10s" node "node-1" eventually reports describe ingestor "retained_source" as "nervix_ingestor_quiesce_buffered_records: 3"
    When extension execution is saturated on every node
    And the entity gate pause for domain "{{domain}}" is released
    Then the background NSPL execution succeeds
    And within "10s" node "node-1" eventually reports describe ingestor "retained_source" as "status: running"
    And within "10s" node "node-1" eventually reports describe ingestor "retained_source" as "nervix_ingestor_quiesce_buffered_records: 3"
    When extension execution is released on every node
    Then within "30s" the relay subscription receives payloads in order
      """
      "seq":1
      "seq":2
      "seq":3
      """
    And within "10s" node "node-1" eventually reports describe ingestor "retained_source" as "nervix_ingestor_quiesce_buffered_records: 0"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @polled_unfolding_waits_for_extension_room
  Scenario Outline: A polled payload waits for the extension workers when the node cannot unfold it
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA polled_event ( user_id I64 );
      CREATE CODEC polled_codec FROM JSON TO SCHEMA polled_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY polled_events SCHEMA polled_event UNBRANCHED;
      CREATE CLIENT polled_http TYPE HTTP CONFIG {
        'endpoint' = '{{mock_http_addr}}/http/{{test_id}}',
        'method' = 'GET',
        'timeout_ms' = 5000
      };
      CREATE INGESTOR polled_source
        FROM HTTP polled_http EVERY 1s
        ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION polled_subscription TO polled_events;
      """
    When extension execution is saturated on every node
    And these NSPL commands are executed
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the unacknowledged source "<source>" ingestor is created
    And extension execution is saturated on every node
    And these NSPL commands are executed
      """
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "unacknowledged_source" on the leader node contains
      """
      ready: true
      """
    When the unacknowledged source "<source>" delivers the payload of user 42
    # Nothing can be unfolded while every extension worker and place in the extension wait queue
    # is held.
    Then the relay subscription does not receive a payload within "3s"
    When extension execution is released on every node
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size | source |
      | 1            | nats   |
      | 3            | nats   |
      | 1            | syslog |
      | 3            | syslog |
      | 1            | zeromq |
      | 3            | zeromq |

  @unacknowledged_unfolding_refused_without_extension_room
  Scenario Outline: An unacknowledged <source> payload the node cannot unfold is refused and counted
    Given unacknowledged source "<source>" is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the unacknowledged source "<source>" ingestor is created
    And extension execution is saturated on every node
    And these NSPL commands are executed
      """
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "unacknowledged_source" on the leader node contains
      """
      ready: true
      """
    When the unacknowledged source "<source>" delivers the payload of user 42
    Then within "30s" the active session observes a server error containing
      """
      the node's bounded execution did not unfold the payload
      """
    And the unacknowledged source ingestor eventually counts 1 refused payload
    When extension execution is released on every node
    And the unacknowledged source "<source>" delivers the payload of user 7
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":7
      """

    Examples:
      | cluster_size | source           |
      | 1            | kafka            |
      | 3            | kafka            |
      | 1            | mqtt             |
      | 3            | mqtt             |
      | 1            | pulsar           |
      | 3            | pulsar           |
      | 1            | redis            |
      | 3            | redis            |
      | 1            | websocket client |
      | 3            | websocket client |

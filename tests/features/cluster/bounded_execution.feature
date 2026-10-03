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

Feature: Relay fan-out
  @relay_fanout_subscriber_loss
  Scenario Outline: Losing a relay subscriber keeps delivery flowing to the relay's other consumers
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the active session
      """
      CREATE SCHEMA notification (
        seq I64
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED CAPACITY 1;

      CREATE CLIENT zeromq_fanout
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE VHOST edge http-{{test_id}}-fanout.example.com;
      CREATE ENDPOINT relay_fanout_ingress ON edge PATH '/relay-fanout' TYPE HTTP;

      CREATE INGESTOR relay_fanout_source
        FROM ENDPOINT relay_fanout_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE EMITTER zeromq_fanout_out FROM notifications TO ZEROMQ zeromq_fanout MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      CREATE SUBSCRIPTION notification_view TO notifications;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-fanout.example.com" path "/relay-fanout"
      """
      {"seq":1}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"seq":1}
      """
    And within "30s" the observed broker receives payloads
      """
      "seq":1
      """
    When these NSPL commands are executed on the active session
      """
      DELETE SUBSCRIPTION notification_view;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-fanout.example.com" path "/relay-fanout"
      """
      {"seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-fanout.example.com" path "/relay-fanout"
      """
      {"seq":3}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-fanout.example.com" path "/relay-fanout"
      """
      {"seq":4}
      """
    Then within "30s" the observed broker receives payloads
      """
      "seq":2
      "seq":3
      "seq":4
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

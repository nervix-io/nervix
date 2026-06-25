Feature: Relay capacity
  Scenario Outline: Relay CAPACITY is persisted and rendered through SHOW CREATE
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        seq I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification CAPACITY 3;
      SHOW CREATE RELAY notifications;
      """
    Then the last command output contains
      """
      CREATE RELAY notifications SCHEMA notification CAPACITY 3;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: ALTER RELAY SET CAPACITY updates a parameterized relay and its active branches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        seq I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch (
        tenant STRING
      );

      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_branch CAPACITY 2;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT relay_capacity_ingress ON edge PATH '/relay-capacity' TYPE HTTP;

      CREATE INGESTOR relay_capacity_source
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM ENDPOINT relay_capacity_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relay-capacity"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relay-capacity"
      """
      {"tenant":"beta","seq":1}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "seq":1,"tenant":"acme"
      "seq":1,"tenant":"beta"
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER RELAY notifications SET CAPACITY 5;
      SHOW CREATE RELAY notifications;
      """
    Then the last command output contains
      """
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_branch CAPACITY 5;
      """
    And within "5s" node "node-1" eventually reports describe relay as "capacity: 5"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'acme');
      """
    And within "5s" node "node-1" eventually reports describe relay as "capacity: 5"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'beta');
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relay-capacity"
      """
      {"tenant":"acme","seq":2}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "seq":2,"tenant":"acme"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @relay_capacity_shrink_preserves_buffered_payloads
  Scenario Outline: Shrinking relay CAPACITY preserves buffered runtime consumer payloads
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        seq I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNPARAMETERIZED CAPACITY 3;

      CREATE CLIENT zeromq_capacity_shrink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE VHOST edge http-{{test_id}}-shrink.example.com;
      CREATE ENDPOINT relay_capacity_shrink_ingress ON edge PATH '/relay-capacity-shrink' TYPE HTTP;

      CREATE INGESTOR relay_capacity_shrink_source
        TO notifications
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH IMMEDIATE
        FROM ENDPOINT relay_capacity_shrink_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER zeromq_capacity_shrink_out
        FROM notifications
        ENCODE USING notification_codec
        TO ZEROMQ zeromq_capacity_shrink ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH IMMEDIATE;

      START;
      """
    And emitter "zeromq_capacity_shrink_out" enters stall mode
    And http payload is posted to node "node-1" with host "http-{{test_id}}-shrink.example.com" path "/relay-capacity-shrink"
      """
      {"seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-shrink.example.com" path "/relay-capacity-shrink"
      """
      {"seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-shrink.example.com" path "/relay-capacity-shrink"
      """
      {"seq":3}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-shrink.example.com" path "/relay-capacity-shrink"
      """
      {"seq":4}
      """
    Then node "node-1" observability metric "nervix_messages_total" with labels eventually equals 4
      """
      target_kind="RELAY"
      target="notifications"
      direction="received"
      relay="notifications"
      """
    And within "5s" DESCRIBE EMITTER "zeromq_capacity_shrink_out" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    When these NSPL commands are executed through the client on the leader node
      """
      ALTER RELAY notifications SET CAPACITY 1;
      """
    And emitter "zeromq_capacity_shrink_out" leaves stall mode
    Then within "5s" the observed broker receives payloads
      """
      "seq":1
      "seq":2
      "seq":3
      "seq":4
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

Feature: Sensitive data
  Scenario Outline: Sensitive relay fields are masked in session subscription output
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        secret STRING SENSITIVE,
        action STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        secret string,
        action string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNPARAMETERIZED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT sensitive_notifications_endpoint
        ON edge
        PATH '/sensitive'
        TYPE HTTP;

      CREATE INGESTOR sensitive_notifications
        TO notifications
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH IMMEDIATE
        FROM ENDPOINT sensitive_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/sensitive"
      """
      {"user_id":42,"secret":"top-secret","action":"OPEN"}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload masks field "secret"
    And the last relay subscription payload contains
      """
      "user_id":42
      "action":"OPEN"
      """
    And the last relay subscription payload does not contain "top-secret"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: leak_sensitive allows explicitly downgrading sensitive data
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        secret STRING SENSITIVE,
        action STRING
      );

      CREATE SCHEMA public_notification (
        user_id I64,
        secret STRING,
        action STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        secret string,
        action string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNPARAMETERIZED;
      CREATE RELAY public_notifications SCHEMA public_notification UNPARAMETERIZED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT sensitive_notifications_endpoint
        ON edge
        PATH '/sensitive'
        TYPE HTTP;

      CREATE INGESTOR sensitive_notifications
        TO notifications
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH IMMEDIATE
        FROM ENDPOINT sensitive_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE ROUTER reveal_notifications
        FROM notifications
        SET notifications.secret = leak_sensitive(notifications.secret)
        DEFAULT TO public_notifications UNPARAMETERIZED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO public_notifications;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/sensitive"
      """
      {"user_id":42,"secret":"top-secret","action":"OPEN"}
      """
    Then the relay subscription receives a payload
      """
      "secret":"top-secret"
      """
    And the last relay subscription payload contains
      """
      "user_id":42
      "action":"OPEN"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Sensitive data cannot flow into non-sensitive fields implicitly
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "would store sensitive data in a non-sensitive output field"
      """
      CREATE UNPACED DOMAIN {{domain}};

      CREATE SCHEMA notification (
        user_id I64,
        secret STRING SENSITIVE
      );

      CREATE SCHEMA public_notification (
        user_id I64,
        secret STRING
      );

      CREATE RELAY notifications SCHEMA notification UNPARAMETERIZED;
      CREATE RELAY public_notifications SCHEMA public_notification UNPARAMETERIZED;

      CREATE ROUTER leak_notifications
        FROM notifications
        DEFAULT TO public_notifications UNPARAMETERIZED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Sensitive data may be emitted to an external sink without leak_sensitive
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        secret STRING SENSITIVE,
        action STRING
      );

      CREATE SCHEMA emitted_notification (
        user_id I64,
        secret STRING,
        action STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        secret string,
        action string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE CODEC emitted_notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA emitted_notification;

      CREATE RELAY notifications SCHEMA notification UNPARAMETERIZED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT sensitive_notifications_endpoint
        ON edge
        PATH '/sensitive'
        TYPE HTTP;

      CREATE INGESTOR sensitive_notifications
        TO notifications
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH IMMEDIATE
        FROM ENDPOINT sensitive_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE EMITTER sensitive_notifications_out
        FROM notifications
        ENCODE USING emitted_notification_codec
        TO ZEROMQ zeromq_main
        SET message.secret = notifications.secret
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/sensitive"
      """
      {"user_id":42,"secret":"top-secret","action":"OPEN"}
      """
    Then the observed broker receives a payload
      """
      {"action":"OPEN","secret":"top-secret","user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

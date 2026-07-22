Feature: Schema type strictness
  Scenario Outline: Branching values must exactly match branch schema field types
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "branch SET compile failed: SET field 'tenant' has expression type Utf8, expected declared output type UInt32"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( tenant STRING );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( tenant string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE SCHEMA tenant_branch ( tenant U32 );
      CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA tenant_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_main TOPIC notifications OFFSET BY CONSUMER GROUP strict_types MODE NO_ACK PARALLEL MAX 10
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: JSON codecs require explicit RFC3339 encoding for string datetime wire fields
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "json field 'created_at' type mismatch"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA orders ( created_at DATETIME );
      CREATE STRICT WIRE JSON SCHEMA orders_wire ( created_at string );
      CREATE CODEC orders_codec FROM WIRE JSON SCHEMA orders_wire TO SCHEMA orders;
      """
    When these NSPL commands are executed
      """
      CREATE STRICT WIRE JSON SCHEMA orders_wire_encoded ( created_at string );
      CREATE CODEC orders_codec_encoded
        FROM WIRE JSON SCHEMA orders_wire_encoded
        TO SCHEMA orders
        ENCODE created_at AS RFC3339;
      """
    When these NSPL commands fail with "json field 'created_at' type mismatch"
      """
      CREATE STRICT WIRE JSON SCHEMA orders_wire_invalid ( created_at number );
      CREATE CODEC orders_codec_invalid
        FROM WIRE JSON SCHEMA orders_wire_invalid
        TO SCHEMA orders
        ENCODE created_at AS RFC3339;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Route-local projections validate explicit output targets
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA inbound_event (
        id STRING,
        legacy STRING
      );
      CREATE SCHEMA projected_event (
        id STRING
      );
      CREATE RELAY inbound_events SCHEMA inbound_event UNBRANCHED;
      CREATE RELAY unknown_set_events SCHEMA projected_event UNBRANCHED;
      CREATE RELAY valid_projected_events SCHEMA projected_event UNBRANCHED;
      """
    When these NSPL commands fail with "SET targets unknown output field 'extra'"
      """
      CREATE DEDUPLICATOR unknown_set_projection
        FROM inbound_events
        DEDUPLICATE ON input.id
        MAX TIME 10m
        UNBRANCHED
        TO unknown_set_events
          SET extra = "x"
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed
      """
      CREATE DEDUPLICATOR valid_projection
        FROM inbound_events
        DEDUPLICATE ON input.id
        MAX TIME 10m
        UNBRANCHED
        TO valid_projected_events
          SET id = input.id
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: NULL assignment requires a declared optional output field
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA nullable_inbound_event (
        id STRING,
        legacy STRING
      );
      CREATE SCHEMA nullable_projected_event (
        id STRING,
        memo STRING OPTIONAL
      );
      CREATE SCHEMA required_projected_event (
        id STRING,
        memo STRING
      );
      CREATE RELAY nullable_inbound_events SCHEMA nullable_inbound_event UNBRANCHED;
      CREATE RELAY nullable_projected_events SCHEMA nullable_projected_event UNBRANCHED;
      CREATE RELAY required_projected_events SCHEMA required_projected_event UNBRANCHED;
      """
    When these NSPL commands fail with "SET field 'memo' may be null but the output field is required"
      """
      CREATE DEDUPLICATOR null_required_projection
        FROM nullable_inbound_events
        DEDUPLICATE ON input.id
        MAX TIME 10m
        UNBRANCHED
        TO required_projected_events
          SET id = input.id,
              memo = NULL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed
      """
      CREATE DEDUPLICATOR null_optional_projection
        FROM nullable_inbound_events
        DEDUPLICATE ON input.id
        MAX TIME 10m
        UNBRANCHED
        TO nullable_projected_events
          SET id = input.id,
              memo = NULL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

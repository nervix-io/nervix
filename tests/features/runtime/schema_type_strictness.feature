Feature: Schema type strictness
  Scenario Outline: Parameterization values must exactly match branch schema field types
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "PARAMETERIZED BY value field 'tenant' type mismatch"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( tenant STRING );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( tenant string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE SCHEMA tenant_branch ( tenant U32 );
      CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main TOPIC notifications OFFSET BY CONSUMER GROUP strict_types MODE NO_ACK PARALLEL MAX 10
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
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

  Scenario Outline: Filter-map projections must explicitly drop source-only fields
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
      CREATE RELAY missing_unset_events SCHEMA projected_event UNBRANCHED;
      CREATE RELAY unknown_set_events SCHEMA projected_event UNBRANCHED;
      CREATE RELAY valid_projected_events SCHEMA projected_event UNBRANCHED;
      """
    When these NSPL commands fail with "source field 'inbound_events.legacy' is not declared in the output schema and must be listed in UNSET"
      """
      CREATE DEDUPLICATOR missing_unset_projection
        FROM inbound_events
        TO missing_unset_events
          SET missing_unset_events.id = inbound_events.id
        UNBRANCHED
        DEDUPLICATE ON inbound_events.id
        MAX TIME 10m
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands fail with "SET field 'extra' is not declared in the output schema"
      """
      CREATE DEDUPLICATOR unknown_set_projection
        FROM inbound_events
        TO unknown_set_events
          SET unknown_set_events.extra = "x"
          UNSET inbound_events.legacy
        UNBRANCHED
        DEDUPLICATE ON inbound_events.id
        MAX TIME 10m
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed
      """
      CREATE DEDUPLICATOR valid_projection
        FROM inbound_events
        TO valid_projected_events
          SET valid_projected_events.id = inbound_events.id
          UNSET inbound_events.legacy
        UNBRANCHED
        DEDUPLICATE ON inbound_events.id
        MAX TIME 10m
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
        TO required_projected_events
          SET required_projected_events.memo = NULL
          UNSET nullable_inbound_events.legacy
        UNBRANCHED
        DEDUPLICATE ON nullable_inbound_events.id
        MAX TIME 10m
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed
      """
      CREATE DEDUPLICATOR null_optional_projection
        FROM nullable_inbound_events
        TO nullable_projected_events
          SET nullable_projected_events.memo = NULL
          UNSET nullable_inbound_events.legacy
        UNBRANCHED
        DEDUPLICATE ON nullable_inbound_events.id
        MAX TIME 10m
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

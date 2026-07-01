Feature: HTTP codec ingestion
  Scenario Outline: HTTP endpoint ingestor delivers a single <wire_format> payload through a schemaful codec
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
        user_id I64
      );
        CREATE STRICT WIRE <wire_format> SCHEMA notification_wire (
        tenant <tenant_wire_type>,
        user_id <user_id_wire_type>
      );
        CREATE CODEC notification_codec
        FROM WIRE <wire_format> SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA tenant_user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { tenant = notifications.tenant, user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    When http payload encoded as "<wire_format>" is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"tenant":"acme","user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme","user_id":42}'

    Examples:
      | cluster_size | replica_count | wire_format | tenant_wire_type | user_id_wire_type |
      | 1            | 0             | JSON        | string           | integer           |
      | 1            | 0             | AVRO        | STRING           | LONG              |
      | 3            | 0             | JSON        | string           | integer           |
      | 3            | 0             | AVRO        | STRING           | LONG              |
      | 3            | 1             | JSON        | string           | integer           |
      | 3            | 1             | AVRO        | STRING           | LONG              |

  Scenario Outline: HTTP endpoint ingestor delivers <wire_format> array and vector fields through a schemaful codec
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA metrics (
        device STRING,
        cpu_last_64 <cpu_type>,
        labels <labels_type> OPTIONAL
      );
        CREATE STRICT WIRE <wire_format> SCHEMA metrics_wire (
        device STRING,
        cpu_last_64 ARRAY,
        labels ARRAY OPTIONAL
      );
        CREATE CODEC metrics_codec
        FROM WIRE <wire_format> SCHEMA metrics_wire
        TO SCHEMA metrics;
        CREATE IF NOT EXISTS SCHEMA device_branch ( device STRING );
        CREATE IF NOT EXISTS BRANCH by_metrics_ingestor SCHEMA device_branch TTL 5m;
        CREATE RELAY metrics_stream SCHEMA metrics BRANCHED BY by_metrics_ingestor;
        CREATE VHOST edge http-arrays-{{test_id}}.example.com;
        CREATE ENDPOINT metrics_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR metrics_ingestor
        TO metrics_stream
        DECODE USING metrics_codec
        BRANCHED BY by_metrics_ingestor VALUES { device = metrics_stream.device }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT metrics_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO metrics_stream;
        START;
      """
    When http payload encoded as "<wire_format>" is posted to host "http-arrays-{{test_id}}.example.com" path "/ingest"
      """
      {"device":"edge-1","cpu_last_64":[1.0,2.5,3.25],"labels":["prod","api"]}
      """
    Then the relay subscription receives a payload
      """
      {"cpu_last_64":[1.0,2.5,3.25],"device":"edge-1","labels":["prod","api"]}
      """
    And the last relay subscription payload contains key fragment '{"device":"edge-1"}'

    Examples:
      | cluster_size | wire_format | cpu_type      | labels_type |
      | 1            | JSON        | ARRAY<F32, 3> | VEC<STRING> |
      | 1            | AVRO        | ARRAY<F32, 3> | VEC<STRING> |
      | 3            | JSON        | ARRAY<F32, 3> | VEC<STRING> |
      | 3            | AVRO        | ARRAY<F32, 3> | VEC<STRING> |

  Scenario Outline: Schemaful <wire_format> wire schemas enforce strictness for extra fields
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE STRICT WIRE <wire_format> SCHEMA strict_notification_wire (
        tenant <tenant_wire_type>,
        user_id <user_id_wire_type>
      );

      CREATE LOOSE WIRE <wire_format> SCHEMA loose_notification_wire (
        tenant <tenant_wire_type>,
        user_id <user_id_wire_type>
      );

      CREATE CODEC strict_notification_codec
        FROM WIRE <wire_format> SCHEMA strict_notification_wire
        TO SCHEMA notification;

      CREATE CODEC loose_notification_codec
        FROM WIRE <wire_format> SCHEMA loose_notification_wire
        TO SCHEMA notification;

      CREATE RELAY strict_notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY loose_notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge strict-loose-{{test_id}}.example.com;

      CREATE ENDPOINT strict_ingress
        ON edge
        PATH '/strict'
        TYPE HTTP;

      CREATE ENDPOINT loose_ingress
        ON edge
        PATH '/loose'
        TYPE HTTP;

      CREATE INGESTOR strict_notifications_source
        TO strict_notifications
        DECODE USING strict_notification_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT strict_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE INGESTOR loose_notifications_source
        TO loose_notifications
        DECODE USING loose_notification_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT loose_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO strict_notifications;
      SUBSCRIBE SESSION TO loose_notifications;
      START;
      """
    When http payload encoded as "<wire_format>" is posted to host "strict-loose-{{test_id}}.example.com" path "/strict"
      """
      {"tenant":"acme","user_id":42,"ignored":"drop-me"}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload encoded as "<wire_format>" is posted to host "strict-loose-{{test_id}}.example.com" path "/loose"
      """
      {"tenant":"acme","user_id":42,"ignored":"drop-me"}
      """
    Then the relay subscription receives a payload
      """
      {"tenant":"acme","user_id":42}
      """
    And the last relay subscription payload does not contain "ignored"

    Examples:
      | cluster_size | wire_format | tenant_wire_type | user_id_wire_type |
      | 1            | JSON        | string           | integer           |
      | 3            | JSON        | string           | integer           |
      | 1            | CBOR        | string           | integer           |
      | 3            | CBOR        | string           | integer           |

  Scenario Outline: HTTP endpoint ingestor delivers <wire_format> array and vector fields through a JAQ-native codec
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA metrics (
        device STRING,
        cpu_last_64 <cpu_type>,
        labels <labels_type> OPTIONAL
      );
        CREATE CODEC metrics_codec
        FROM <wire_format>
        TO SCHEMA metrics
        WITH JAQ TRANSFORMATION '.';
        CREATE IF NOT EXISTS SCHEMA device_branch ( device STRING );
        CREATE IF NOT EXISTS BRANCH by_metrics_ingestor SCHEMA device_branch TTL 5m;
        CREATE RELAY metrics_stream SCHEMA metrics BRANCHED BY by_metrics_ingestor;
        CREATE VHOST edge http-binary-arrays-{{test_id}}.example.com;
        CREATE ENDPOINT metrics_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR metrics_ingestor
        TO metrics_stream
        DECODE USING metrics_codec
        BRANCHED BY by_metrics_ingestor VALUES { device = metrics_stream.device }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT metrics_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO metrics_stream;
        START;
      """
    When http payload encoded as "<wire_format>" is posted to host "http-binary-arrays-{{test_id}}.example.com" path "/ingest"
      """
      {"device":"edge-1","cpu_last_64":[1.0,2.5,3.25],"labels":["prod","api"]}
      """
    Then the relay subscription receives a payload
      """
      {"cpu_last_64":[1.0,2.5,3.25],"device":"edge-1","labels":["prod","api"]}
      """
    And the last relay subscription payload contains key fragment '{"device":"edge-1"}'

    Examples:
      | cluster_size | wire_format | cpu_type      | labels_type |
      | 1            | CBOR        | ARRAY<F32, 3> | VEC<STRING> |
      | 3            | CBOR        | ARRAY<F32, 3> | VEC<STRING> |

  Scenario Outline: HTTP endpoint ingestor delivers a single <wire_format> payload through a JAQ-native codec
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
        user_id I64
      );
        CREATE CODEC notification_codec
        FROM <wire_format>
        TO SCHEMA notification
        WITH JAQ TRANSFORMATION '.';
        CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA tenant_user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { tenant = notifications.tenant, user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    When http payload encoded as "<wire_format>" is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"tenant":"acme","user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme","user_id":42}'

    Examples:
      | cluster_size | replica_count | wire_format |
      | 1            | 0             | CBOR        |
      | 3            | 0             | CBOR        |
      | 3            | 1             | CBOR        |

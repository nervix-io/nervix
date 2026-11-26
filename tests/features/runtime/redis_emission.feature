Feature: Redis emission
  @retained_task_handles
  Scenario Outline: Redis emitter publishes JSON payloads from a relay
    Given Redis is running
    And MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Redis channel "<channel>_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE WIRE JSON CODEC notification_codec
        FROM WIRE SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS PARAMETRIZER by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE REDIS CLIENT redis_main
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = '{{redis_addr}}'
        };
        CREATE REDIS PUBSUB EMITTER redis_notifications FROM notifications TO redis_main CHANNEL '<channel>_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "redis_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "redis_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "redis_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count | channel           |
      | 1            | 0             | notifications_out |
      | 3            | 0             | notifications_out |
      | 3            | 1             | notifications_out |
      | 1            | 0             | Orders:EU/*       |
      | 3            | 0             | Orders:EU/*       |

  @redis_publish_refusal
  Scenario Outline: A Redis emitter whose user may publish to no channel gives up its row with the server's error prefix
    Given Redis is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And a Redis user that may publish to no channel connects at placeholder "refused_redis_addr"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA failed_event (
        seq I64,
        error_code STRING,
        operation STRING,
        external_code STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY failed_events SCHEMA failed_event UNBRANCHED;
      CREATE VHOST edge redis-refusal-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = '{{refused_redis_addr}}'
      };
      CREATE REDIS PUBSUB EMITTER refused FROM events
        TO redis_main CHANNEL 'refused_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 2s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET seq = input.seq,
              error_code = error.class,
              operation = error.operation,
              external_code = error.external_code
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_events_subscription TO failed_events;
      START;
      """
    # The server answers every publish of the user with a NOPERM error, which the emitter retries
    # as a failure of its destination until GIVE UP AFTER passes. The row then fails as unavailable
    # and names the prefix of the last error the server answered.
    And http payload is posted to host "redis-refusal-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "error_code":"unavailable" | "operation":"publish" | "external_code":"NOPERM"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

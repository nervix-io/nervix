Feature: Emitter metrics

  Scenario Outline: DESCRIBE EMITTER and Prometheus report emitter traffic metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_emitter_metrics_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_emitter_metrics_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT emitter_metrics_ingress ON edge PATH '/emitter-metrics' TYPE HTTP;
        CREATE HTTP ENDPOINT INGESTOR emitter_metrics_source
        FROM emitter_metrics_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_emitter_metrics_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE ZEROMQ CLIENT zeromq_main
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
        CREATE ZEROMQ PUSH EMITTER emitter_metrics_node FROM notifications TO zeromq_main MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/emitter-metrics"
      """
      {"user_id":42}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/emitter-metrics"
      """
      {"user_id":43}
      """
    Then the observed broker receives a payload
      """
      "user_id":
      """
    When these Plexus commands are executed
      """
      DESCRIBE EMITTER emitter_metrics_node;
      """
    Then the last command output contains
      """
      emitter: emitter_metrics_node
      """
    And the last command output owner is saved as placeholder "emitter_metrics_owner"
    And the last command output contains
      """
      from: notifications
      """
    And the last command output contains
      """
      to: ZEROMQ PUSH zeromq_main
      """
    And the last command output contains
      """
      destination:
      """
    And the last command output contains
      """
      metrics:
      """
    And the last command output contains
      """
      messages_total received relay=notifications physical_node={{emitter_metrics_owner}} total=2
      """
    And the last command output contains
      """
      messages_total sent relay=notifications physical_node={{emitter_metrics_owner}} total=2
      """
    And the last command output metric "messages_total" "received" relay "notifications" physical node "{{emitter_metrics_owner}}" has values
      """
      total=2
      """
    And the last command output metric "messages_total" "sent" relay "notifications" physical node "{{emitter_metrics_owner}}" has values
      """
      total=2
      """
    And the last command output metric "batches_total" "received" relay "notifications" physical node "{{emitter_metrics_owner}}" has values
      """
      total=2
      """
    And the last command output metric "batches_total" "sent" relay "notifications" physical node "{{emitter_metrics_owner}}" has numeric values
      """
      total>=1
      total<=2
      """
    And node "{{emitter_metrics_owner}}" observability path "/metrics" eventually responds with 200 and contains 'target_kind="EMITTER"'
    And node "{{emitter_metrics_owner}}" observability path "/metrics" eventually responds with 200 and contains 'target="emitter_metrics_node"'
    And node "{{emitter_metrics_owner}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="EMITTER"
      target="emitter_metrics_node"
      direction="received"
      relay="notifications"
      """
    And node "{{emitter_metrics_owner}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="EMITTER"
      target="emitter_metrics_node"
      direction="sent"
      relay="notifications"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Redis destination attempts start at zero and report recovery through the owner
    Given Redis is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Redis channel "metrics_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR events_in
        FROM SCHEMA event MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 2
        CONFIG { 'addr' = '{{redis_addr}}' };
      CREATE REDIS PUBSUB EMITTER metrics_out FROM events
        TO redis_main CHANNEL 'metrics_{{test_id}}' MODE NO ACK
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "metrics_out" on the leader node contains
      """
      destination:
      """
    And the last command output owner is saved as placeholder "metrics_owner"
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 0
      """
      emitter="metrics_out"
      connector="REDIS"
      entity="PUBSUB"
      operation="publish"
      outcome="succeeded"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 0
      """
      emitter="metrics_out"
      operation="publish"
      outcome="transient"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 0
      """
      emitter="metrics_out"
      operation="publish"
      outcome="misconfigured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 0
      """
      emitter="metrics_out"
      operation="publish"
      outcome="violation"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_duration_seconds_count" with labels eventually equals 0
      """
      emitter="metrics_out"
      operation="publish"
      """
    And client "publisher" is connected to the leader node
    When client "publisher" opens producer "events" on ingestor "events_in" expecting fields "seq I64"
    And emitter "metrics_out" enters stall mode
    And producer "events" submits batch "first" with rows
      | seq |
      | 1   |
      | 2   |
      | 3   |
    Then node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually reaches at least 1
      """
      emitter="metrics_out"
      operation="publish"
      outcome="transient"
      """
    When emitter "metrics_out" leaves stall mode
    Then batch "first" completes
    And within "30s" the observed broker receives exactly these payloads
      """
      {"seq":1}
      {"seq":2}
      {"seq":3}
      """
    And within "30s" DESCRIBE EMITTER "metrics_out" on the leader node contains
      """
      messages_total sent relay=events physical_node={{metrics_owner}} total=3
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 1
      """
      emitter="metrics_out"
      operation="publish"
      outcome="succeeded"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_duration_seconds_count" with labels eventually reaches at least 2
      """
      emitter="metrics_out"
      operation="publish"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 0
      """
      emitter="metrics_out"
      operation="commit"
      outcome="succeeded"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels is saved as placeholder "recovered_transient_attempts"
      """
      emitter="metrics_out"
      operation="publish"
      outcome="transient"
      """
    When producer "events" submits batch "second" with rows
      | seq |
      | 4   |
    Then batch "second" completes
    And the observed broker receives a payload
      """
      {"seq":4}
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 2
      """
      emitter="metrics_out"
      operation="publish"
      outcome="succeeded"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels equals saved placeholder "recovered_transient_attempts"
      """
      emitter="metrics_out"
      operation="publish"
      outcome="transient"
      """
    And DESCRIBE EMITTER "metrics_out" from every node contains
      """
      nervix_emitter_publish_attempts_total{domain="{{domain}}",emitter="metrics_out",physical_node_id="{{metrics_owner}}",connector="REDIS",entity="PUBSUB",operation="publish",outcome="succeeded"} total=2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A receiver that refuses the OTEL endpoint counts one misconfigured destination attempt
    Given HTTP receiver "collector" is running
    And HTTP receiver "collector" answers unscripted requests with "respond 401"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR events_in
        FROM SCHEMA event MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE OTEL CLIENT collector CONFIG {
        'endpoint' = '{{http_receiver.collector}}', 'protocol' = 'http/protobuf',
        'timeout_ms' = 5000
      };
      CREATE OTEL LOGS EMITTER refused_logs FROM events TO collector
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'time' = now(), 'body' = 'event' } ATTRIBUTES { 'event.seq' = input.seq }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "publisher" is connected to the leader node
    When client "publisher" opens producer "events" on ingestor "events_in" expecting fields "seq I64"
    And producer "events" submits batch "refused" with rows
      | seq |
      | 1   |
    Then HTTP receiver "collector" eventually receives at least 1 requests
    And within "30s" DESCRIBE EMITTER "refused_logs" on the leader node contains
      """
      destination:
      """
    And the last command output owner is saved as placeholder "metrics_owner"
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 1
      """
      emitter="refused_logs"
      connector="OTEL"
      entity="LOGS"
      operation="publish"
      outcome="misconfigured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_duration_seconds_count" with labels eventually equals 1
      """
      emitter="refused_logs"
      operation="publish"
      """
    And HTTP receiver "collector" has captured exactly 1 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An answer for an unknown record counts one destination contract violation
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR events_in
        FROM SCHEMA event MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG { 'addr' = '{{zeromq_emit_addr}}', 'bind' = false };
      CREATE ZEROMQ PUSH EMITTER bad_answer FROM events TO destination
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "publisher" is connected to the leader node
    When client "publisher" opens producer "events" on ingestor "events_in" expecting fields "seq I64"
    And emitter "bad_answer" answers its next record write with an unknown record
    And producer "events" submits batch "foreign_answer" with rows
      | seq |
      | 1   |
      | 2   |
    Then within "30s" DESCRIBE EMITTER "bad_answer" on the leader node contains
      """
      destination:
      """
    And the last command output owner is saved as placeholder "metrics_owner"
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 1
      """
      emitter="bad_answer"
      connector="ZEROMQ"
      entity="PUSH"
      operation="publish"
      outcome="violation"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually equals 0
      """
      emitter="bad_answer"
      operation="publish"
      outcome="succeeded"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_duration_seconds_count" with labels eventually equals 1
      """
      emitter="bad_answer"
      operation="publish"
      """
    And DESCRIBE EMITTER "bad_answer" from every node contains
      """
      nervix_emitter_publish_attempts_total{domain="{{domain}}",emitter="bad_answer",physical_node_id="{{metrics_owner}}",connector="ZEROMQ",entity="PUSH",operation="publish",outcome="violation"} total=1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

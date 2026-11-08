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
        CREATE WIRE JSON CODEC notification_codec
        FROM WIRE SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_emitter_metrics_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_emitter_metrics_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE HTTP ENDPOINT emitter_metrics_ingress ON edge PATH '/emitter-metrics';
        CREATE HTTP ENDPOINT INGESTOR emitter_metrics_ingestor
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

  @emitter_outcomes
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
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
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
    And node "{{metrics_owner}}" observability metric "nervix_emitter_opens_total" with labels eventually equals 1
      """
      emitter="metrics_out"
      outcome="succeeded"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_transient_failure" with labels eventually equals 0
      """
      emitter="metrics_out"
      """
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
    Given client "publisher" is connected to the leader node
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
    And node "{{metrics_owner}}" observability metric "nervix_emitter_transient_failure" with labels eventually equals 1
      """
      emitter="metrics_out"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_retries_total" with labels eventually reaches at least 1
      """
      emitter="metrics_out"
      cause="publish"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_retry_wait_seconds_total" with labels eventually becomes positive
      """
      emitter="metrics_out"
      """
    When emitter "metrics_out" leaves stall mode
    Then batch "first" completes
    And node "{{metrics_owner}}" observability metric "nervix_emitter_transient_failure" with labels eventually equals 0
      """
      emitter="metrics_out"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_records_retried_total" with labels eventually equals 3
      """
      emitter="metrics_out"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_records_retried_total" with labels is saved as placeholder "recovered_retried_records"
      """
      emitter="metrics_out"
      """
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
    And node "{{metrics_owner}}" observability metric "nervix_emitter_records_retried_total" with labels equals saved placeholder "recovered_retried_records"
      """
      emitter="metrics_out"
      """
    And DESCRIBE EMITTER "metrics_out" from every node contains
      """
      nervix_emitter_publish_attempts_total{domain="{{domain}}",emitter="metrics_out",physical_node_id="{{metrics_owner}}",connector="REDIS",entity="PUBSUB",operation="publish",outcome="succeeded"} total=2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_outcomes
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
    And node "{{metrics_owner}}" observability metric "nervix_emitter_records_failed_total" with labels eventually equals 1
      """
      emitter="refused_logs"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_records_retried_total" with labels eventually equals 0
      """
      emitter="refused_logs"
      """


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
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
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

  @emitter_outcomes
  Scenario Outline: An HTTP record refusal is counted once without failing the other record
    Given HTTP receiver "rejections" is running
    And HTTP receiver "rejections" answers with
      """
      respond 422
      respond 204
      """
    And HTTP receiver "rejections" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA rejected (seq I64, code STRING);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR events_in
        FROM SCHEMA event MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.rejections}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER rejected_out FROM events
        TO api METHOD 'POST' PATH '/events' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        WITHOUT BODY FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events SET seq = input.seq, code = error.code
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejections TO rejected_events;
      START;
      """
    And client "publisher" is connected to the leader node
    When client "publisher" opens producer "events" on ingestor "events_in" expecting fields "seq I64"
    And producer "events" submits batch "records" with rows
      | seq |
      | 1   |
      | 2   |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "code":"external"
      """
    And HTTP receiver "rejections" eventually receives at least 2 requests
    And within "30s" DESCRIBE EMITTER "rejected_out" on the leader node contains
      """
      destination:
      """
    And the last command output owner is saved as placeholder "rejection_owner"
    And node "{{rejection_owner}}" observability metric "nervix_emitter_records_rejected_total" with labels eventually equals 1
      """
      emitter="rejected_out"
      code="external"
      """
    And node "{{rejection_owner}}" observability metric "nervix_emitter_records_failed_total" with labels eventually equals 0
      """
      emitter="rejected_out"
      """
    And node "{{rejection_owner}}" observability metric "nervix_emitter_records_retried_total" with labels eventually equals 0
      """
      emitter="rejected_out"
      """
    And HTTP receiver "rejections" has captured exactly 2 requests
    And DESCRIBE EMITTER "rejected_out" from every node contains
      """
      nervix_emitter_records_rejected_total{domain="{{domain}}",emitter="rejected_out",physical_node_id="{{rejection_owner}}",connector="HTTP",entity="REQUEST",code="external"} total=1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_retry_at_replace @emitter_outcomes
  Scenario Outline: Replacing an emitter counts the pending record it retries before its scheduled backoff
    Given HTTP receiver "retry_at_replace" is running
    And HTTP receiver "retry_at_replace" answers with
      """
      respond 503
      respond 204
      """
    And HTTP receiver "retry_at_replace" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge retry-replace-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR events_in FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TIMESTAMP NOW TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.retry_at_replace}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER retrying FROM events
        TO api METHOD 'POST' PATH '/events' MODE ACK RETRY POLICY BACKOFF 5m MAX 5m
        WITHOUT BODY FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to node "node-1" with host "retry-replace-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then HTTP receiver "retry_at_replace" eventually receives at least 1 requests
    And within "30s" DESCRIBE EMITTER "retrying" on the leader node contains
      """
      destination:
      """
    And the last command output owner is saved as placeholder "retry_owner"
    And node "{{retry_owner}}" observability metric "nervix_emitter_retries_total" with labels eventually equals 1
      """
      emitter="retrying"
      cause="publish"
      """
    And node "{{retry_owner}}" observability metric "nervix_emitter_records_retried_total" with labels eventually equals 0
      """
      emitter="retrying"
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER HTTP REQUEST EMITTER retrying SET PATH '/replacement';
      """
    Then HTTP receiver "retry_at_replace" has captured exactly 2 requests
    And HTTP receiver "retry_at_replace" request 2 is
      """
      POST /events
      """
    And node "{{retry_owner}}" observability metric "nervix_emitter_records_retried_total" with labels eventually equals 1
      """
      emitter="retrying"
      """
    And node "{{retry_owner}}" observability metric "nervix_emitter_transient_failure" with labels eventually equals 0
      """
      emitter="retrying"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_outcomes
  Scenario Outline: A failed transport open is counted and its gauge ends with the emitter
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And a silent endpoint "opening_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = 'tcp://{{opening_peer}}', 'connect_timeout_ms' = 1000
      };
      CREATE ZEROMQ PUSH EMITTER opening FROM events TO destination
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "opening_peer" eventually accepts a connection
    And within "30s" DESCRIBE EMITTER "opening" on the leader node contains
      """
      within connect_timeout_ms 1s
      """
    And the last command output owner is saved as placeholder "opening_owner"
    And node "{{opening_owner}}" observability metric "nervix_emitter_opens_total" with labels eventually reaches at least 1
      """
      emitter="opening"
      outcome="failed"
      """
    And node "{{opening_owner}}" observability metric "nervix_emitter_retries_total" with labels eventually reaches at least 1
      """
      emitter="opening"
      cause="connect"
      """
    And node "{{opening_owner}}" observability metric "nervix_emitter_retry_wait_seconds_total" with labels eventually reaches at least 1
      """
      emitter="opening"
      """
    And node "{{opening_owner}}" observability metric "nervix_emitter_transient_failure" with labels eventually equals 1
      """
      emitter="opening"
      """
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then node "{{opening_owner}}" observability metric "nervix_emitter_transient_failure" with labels eventually equals 0
      """
      emitter="opening"
      """
    And node "{{opening_owner}}" observability metric "nervix_emitter_finishes_total" with labels eventually equals 1
      """
      emitter="opening"
      outcome="nothing_held"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

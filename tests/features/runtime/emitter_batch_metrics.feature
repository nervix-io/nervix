Feature: Emitter payload measurements
  Source records, Arrow memory and encoded payloads have distinct units. Preparing a retained
  payload counts once; a retry does not encode or count that payload again.

  @emitter_batch_metrics
  Scenario Outline: Payload measurements distinguish records, containers and byte subdivisions
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
        TIMESTAMP NOW TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG { 'addr' = '{{zeromq_emit_addr}}', 'bind' = false };
      CREATE ZEROMQ PUSH EMITTER measured FROM events TO destination
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL <batch_clause> FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "measured" on the leader node contains
      """
      batch: <batch_policy>
      batch container: <container>
      """
    And the last command output owner is saved as placeholder "metrics_owner"
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payloads_total" with labels eventually equals 0
      """
      emitter="measured"
      """
    Given client "publisher" is connected to the leader node
    When client "publisher" opens producer "events" on ingestor "events_in" expecting fields "seq I64"
    And the emitter of emitter "measured" stalls after resolving 0 records of its next publish
    And producer "events" submits batch "first" with rows
      | seq |
      | 1   |
      | 2   |
      | 3   |
    Then batch "first" completes
    And within "30s" the observed broker receives payloads
      """
      "seq":1
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_publish_attempts_total" with labels eventually reaches at least 1
      """
      emitter="measured"
      operation="publish"
      outcome="transient"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payloads_total" with labels eventually equals <payloads>
      """
      emitter="measured"
      connector="ZEROMQ"
      entity="PUSH"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payload_source_messages_total" with labels eventually equals 3
      """
      emitter="measured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payload_encoded_bytes_total" with labels eventually equals <bytes>
      """
      emitter="measured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payload_size_bound_bytes_total" with labels eventually equals <bytes>
      """
      emitter="measured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_prepared_bytes_total" with labels eventually equals <bytes>
      """
      emitter="measured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payload_splits_total" with labels eventually equals <splits>
      """
      emitter="measured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_messages_per_payload_count" with labels eventually equals <payloads>
      """
      emitter="measured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_work_messages" with labels eventually equals 0
      """
      emitter="measured"
      state="prepared"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_work_messages" with labels eventually equals 0
      """
      emitter="measured"
      state="in_flight"
      """
    And DESCRIBE EMITTER "measured" from every node contains
      """
      payloads: <payloads> source_messages=3 encoded_bytes=<bytes> size_bound_bytes=<bytes>
      """

    Examples:
      | cluster_size | batch_clause                       | batch_policy                 | container   | payloads | bytes | splits |
      | 1            |                                    | none                         | JSON record | 3        | 27    | 0      |
      | 3            |                                    | none                         | JSON record | 3        | 27    | 0      |
      | 1            | BATCH MAX MESSAGES 3 MAX SIZE 1KiB | MAX MESSAGES 3 MAX SIZE 1KiB | JSON array  | 1        | 31    | 0      |
      | 3            | BATCH MAX MESSAGES 3 MAX SIZE 1KiB | MAX MESSAGES 3 MAX SIZE 1KiB | JSON array  | 1        | 31    | 0      |
      | 1            | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | MAX MESSAGES 2 MAX SIZE 1KiB | JSON array  | 2        | 32    | 0      |
      | 3            | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | MAX MESSAGES 2 MAX SIZE 1KiB | JSON array  | 2        | 32    | 0      |
      | 1            | BATCH MAX MESSAGES 3 MAX SIZE 21B  | MAX MESSAGES 3 MAX SIZE 21B  | JSON array  | 2        | 32    | 1      |
      | 3            | BATCH MAX MESSAGES 3 MAX SIZE 21B  | MAX MESSAGES 3 MAX SIZE 21B  | JSON array  | 2        | 32    | 1      |

  @emitter_batch_metrics
  Scenario Outline: Oversize source rows are rejected without a prepared payload
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA rejected_event ( seq I64, error_code STRING, operation STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR events_in
        FROM SCHEMA event MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND
        TIMESTAMP NOW TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG { 'addr' = '{{zeromq_emit_addr}}', 'bind' = false };
      CREATE ZEROMQ PUSH EMITTER measured FROM events TO destination
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL BATCH MAX MESSAGES 3 MAX SIZE 1B FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq, error_code = error.code, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_subscription TO rejected_events;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "measured" on the leader node contains
      """
      batch: MAX MESSAGES 3 MAX SIZE 1B
      """
    And the last command output owner is saved as placeholder "metrics_owner"
    Given client "publisher" is connected to the leader node
    When client "publisher" opens producer "events" on ingestor "events_in" expecting fields "seq I64"
    And producer "events" submits batch "oversize" with rows
      | seq |
      | 1   |
      | 2   |
      | 3   |
    Then batch "oversize" completes
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "error_code":"validation" | "operation":"encode"
      "seq":2 | "error_code":"validation" | "operation":"encode"
      "seq":3 | "error_code":"validation" | "operation":"encode"
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE RELAY rejected_events;
      """
    Then the last command output owner is saved as placeholder "errors_owner"
    And node "{{errors_owner}}" observability metric "nervix_messages_total" with labels eventually equals 3
      """
      target_kind="RELAY"
      target="rejected_events"
      direction="received"
      relay="rejected_events"
      peer_kind="-"
      peer="-"
      """
    And node "{{metrics_owner}}" observability path "/metrics" eventually responds with 200 and contains 'nervix_emitter_payloads_total{'
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payloads_total" with labels eventually equals 0
      """
      emitter="measured"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payload_encoded_bytes_total" with labels eventually equals 0
      """
      emitter="measured"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

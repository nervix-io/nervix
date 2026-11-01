Feature: SQS message metadata
  Scenario Outline: An SQS ingestor projects every declared message field with its exact type
    Given SQS is running
    And a <cluster_size> node nervix cluster is started
    And SQS queue "metadata_{{test_id}}" exists
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (marker STRING);
      CREATE SCHEMA observed (
        marker STRING, message_id STRING OPTIONAL, sent_at DATETIME OPTIONAL,
        receive_count U32 OPTIONAL, first_received_at DATETIME OPTIONAL,
        group_id STRING OPTIONAL, deduplication_id STRING OPTIONAL,
        sequence_number STRING OPTIONAL, sender_id STRING OPTIONAL,
        sent_time_present BOOL, first_receive_present BOOL, sender_present BOOL, group_absent BOOL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (marker string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA observed UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1',
        'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret',
        'timeout_ms' = 10000
      };
      CREATE SQS QUEUE INGESTOR sqs_in FROM sqs_main QUEUE 'metadata_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL
          SET message_id = metadata.message_id, sent_at = metadata.sent_at,
            receive_count = metadata.receive_count, first_received_at = metadata.first_received_at,
            group_id = metadata.group_id, deduplication_id = metadata.deduplication_id,
            sequence_number = metadata.sequence_number, sender_id = metadata.sender_id,
            sent_time_present = NOT is_null(metadata.sent_at),
            first_receive_present = NOT is_null(metadata.first_received_at),
            sender_present = NOT is_null(metadata.sender_id), group_absent = is_null(metadata.group_id)
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observed TO events;
      START;
      """
    When SQS message is published to queue "metadata_{{test_id}}"
      """
      {"marker":"metadata-{{test_id}}"}
      """
    Then within "60s" the relay subscription receives a payload
      """
      "receive_count":1
      """
    And the last relay subscription payload contains
      """
      "sent_time_present":true
      """
    And the last relay subscription payload contains
      """
      "first_receive_present":true
      """
    And the last relay subscription payload contains
      """
      "sender_present":true
      """
    And the last relay subscription payload contains
      """
      "group_absent":true
      """
    And the last relay subscription payload contains
      """
      "message_id":"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: SQS sent time feeds independent branch duration windows
    Given SQS is running
    And a <cluster_size> node nervix cluster is started
    And SQS queue "windows_{{test_id}}.fifo" exists
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value I64);
      CREATE SCHEMA timed_event (tenant STRING, value I64, sent_at DATETIME OPTIONAL);
      CREATE SCHEMA summary (tenant STRING, samples I64, total I64, sent_at DATETIME OPTIONAL);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m;
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA timed_event BRANCHED BY tenants;
      CREATE RELAY summaries SCHEMA summary BRANCHED BY tenants;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1',
        'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret',
        'timeout_ms' = 10000
      };
      CREATE SQS QUEUE INGESTOR sqs_in FROM sqs_main QUEUE 'windows_{{test_id}}.fifo'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec TIMESTAMP AT metadata.sent_at
        TO events INHERIT ALL SET sent_at = metadata.sent_at
          BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR totals FROM events WIDTH 1s DURATION STEP 1s DURATION
        BRANCHED BY tenants
        TO summaries SET tenant = FIRST(input.tenant), samples = COUNT(input.value),
          total = SUM(input.value), sent_at = FIRST(input.sent_at)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observed TO summaries;
      """
    When a redelivered SQS FIFO message with group "windows" and deduplication id "window-{{test_id}}" is ready in queue "windows_{{test_id}}.fifo"
      """
      [{"tenant":"acme","value":10},{"tenant":"beta","value":100},{"tenant":"acme","value":20},{"tenant":"beta","value":200}]
      """
    And these Plexus statements are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "tenant":"acme" | "samples":2 | "total":30 | "sent_at":"{{sqs_sent_at}}"
      key={"tenant":"beta"} | "tenant":"beta" | "samples":2 | "total":300 | "sent_at":"{{sqs_sent_at}}"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A redelivered SQS message is routed by its receive count and preserves its FIFO identity and times
    Given SQS is running
    And a <cluster_size> node nervix cluster is started
    And SQS queue "redelivered_{{test_id}}.fifo" exists
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (marker STRING);
      CREATE SCHEMA retried (
        marker STRING, message_id STRING OPTIONAL, sent_at DATETIME OPTIONAL,
        receive_count U32 OPTIONAL, first_received_at DATETIME OPTIONAL,
        group_id STRING OPTIONAL, deduplication_id STRING OPTIONAL,
        sequence_number STRING OPTIONAL, sender_id STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (marker string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY ordinary SCHEMA event UNBRANCHED;
      CREATE RELAY retries SCHEMA retried UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1',
        'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret',
        'timeout_ms' = 10000
      };
      CREATE SQS QUEUE INGESTOR sqs_in FROM sqs_main QUEUE 'redelivered_{{test_id}}.fifo'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO ordinary INHERIT ALL WHERE metadata.receive_count = (1 AS U32)
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        TO retries INHERIT ALL
          SET message_id = metadata.message_id, sent_at = metadata.sent_at,
            receive_count = metadata.receive_count, first_received_at = metadata.first_received_at,
            group_id = metadata.group_id, deduplication_id = metadata.deduplication_id,
            sequence_number = metadata.sequence_number, sender_id = metadata.sender_id
          WHERE metadata.receive_count >= (2 AS U32)
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION retried TO retries;
      """
    When a redelivered SQS FIFO message with group "tenant-{{test_id}}" and deduplication id "order-{{test_id}}" is ready in queue "redelivered_{{test_id}}.fifo"
      """
      {"marker":"retry-{{test_id}}"}
      """
    And these Plexus statements are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "marker":"retry-{{test_id}}" | "receive_count":2 | "message_id":"{{sqs_message_id}}" | "sent_at":"{{sqs_sent_at}}" | "first_received_at":"{{sqs_first_received_at}}" | "group_id":"tenant-{{test_id}}" | "deduplication_id":"order-{{test_id}}" | "sequence_number":"{{sqs_sequence_number}}" | "sender_id":"{{sqs_sender_id}}"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

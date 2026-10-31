Feature: SQS ingestion
  Scenario Outline: SQS ingestor delivers JSON payloads to a subscribed relay
    Given SQS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "notifications_{{test_id}}" exists
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
        CREATE IF NOT EXISTS BRANCH by_sqs_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_sqs_notifications;
        CREATE SQS CLIENT sqs_main
        CONFIG {
          'endpoint' = '{{sqs_endpoint}}',
          'region' = 'us-east-1'
        };
        CREATE SQS QUEUE INGESTOR sqs_notifications
        FROM sqs_main QUEUE 'notifications_{{test_id}}' INSTANCES <instances> MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_sqs_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And SQS message is published to queue "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | instances | replica_count |
      | 1            | 1         | 0             |
      | 1            | 2         | 0             |
      | 3            | 1         | 0             |
      | 3            | 2         | 0             |
      | 3            | 1         | 1             |
      | 3            | 2         | 1             |

  Scenario Outline: SQS ingestor reports transient source failures and recovers
    Given SQS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "notifications_reconnect_{{test_id}}" exists
    When ingestor "sqs_notifications" enters fault mode
    And these Plexus commands are executed
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
        CREATE IF NOT EXISTS BRANCH by_sqs_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_sqs_notifications;
        CREATE SQS CLIENT sqs_main
        CONFIG {
          'endpoint' = '{{sqs_endpoint}}',
          'region' = 'us-east-1'
        };
        CREATE SQS QUEUE INGESTOR sqs_notifications
        FROM sqs_main QUEUE 'notifications_reconnect_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_sqs_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "sqs_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "sqs_notifications" leaves fault mode
    And SQS message is published to queue "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "sqs_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: An SQS ingestor created while its service is unreachable consumes once the service returns
    Given SQS is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And SQS is forwarded as "sqs.nervix.test" from the fixture addresses "127.0.5.1"
    And the DNS fixture answers "sqs.nervix.test" with addresses "127.0.5.1"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "outage_{{test_id}}" exists
    When the TCP forwarder at "127.0.5.1" stops
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_forwarded_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE SQS QUEUE INGESTOR sqs_in
        FROM sqs_main QUEUE 'outage_{{test_id}}' INSTANCES 1
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "sqs_in" on the leader node contains
      """
      transient error: failed to resolve SQS queue 'outage_{{test_id}}'
      """
    When the TCP forwarder at "127.0.5.1" restarts
    And SQS message is published to queue "outage_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """
    And within "30s" DESCRIBE INGESTOR "sqs_in" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An SQS ingestor created for a queue that does not exist reports the queue by name
    Given SQS is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE SQS QUEUE INGESTOR sqs_in
        FROM sqs_main QUEUE 'missing_{{test_id}}' INSTANCES 1
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "sqs_in" on the leader node contains
      """
      transient error: SQS queue 'missing_{{test_id}}' does not exist
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @sqs_receive_at_quiesce @deloxide_stress
  Scenario Outline: An SQS receive in flight when its ingestor quiesces delivers its message without waiting for the visibility timeout
    Given SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And entity gate deadline is configured as "120s"
    And a <cluster_size> node nervix cluster is started
    And SQS is reached through a forwarder that can hold an answer
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "held_{{test_id}}" exists with a visibility timeout of "600s"
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands are executed
      """
      CREATE SCHEMA held_event (marker STRING);
      CREATE WIRE JSON SCHEMA held_event_wire MODE STRICT (marker string);
      CREATE CODEC held_event_codec FROM WIRE JSON SCHEMA held_event_wire TO SCHEMA held_event;
      CREATE CODEC held_event_codec_v2 FROM WIRE JSON SCHEMA held_event_wire TO SCHEMA held_event;
      CREATE RELAY held_events SCHEMA held_event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_held_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE SQS QUEUE INGESTOR sqs_in
        FROM sqs_main QUEUE 'held_{{test_id}}' INSTANCES 1
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING held_event_codec
        TO held_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION held_subscription TO held_events;
      START;
      """
    And the SQS forwarder holds the answer that carries "held-{{test_id}}"
    And SQS message is published to queue "held_{{test_id}}"
      """
      {"marker":"held-{{test_id}}"}
      """
    Then the SQS forwarder holds that answer
    When these Plexus commands begin executing in the background
      """
      ALTER SQS QUEUE INGESTOR sqs_in SET DECODE USING held_event_codec_v2;
      """
    And the SQS forwarder releases that answer once the entity gate pause for domain "{{domain}}" is reached
    Then within "60s" the relay subscription receives a payload
      """
      "marker":"held-
      """
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # ACK PARALLEL MAX 10 asks each receive for up to ten messages and keeps gathering until its batch
  # is full or its BATCH TIMEOUT ends. A stalled emitter holds the batch's acknowledgement, so the
  # whole batch stays in flight: ten messages invisible to every other receive. Once the emitter
  # resumes, the batch is acknowledged with one DeleteMessageBatch request, never one DeleteMessage
  # request per message.
  @sqs_parallel_batch @deloxide_stress
  Scenario Outline: SQS ACK PARALLEL takes up to its MAX of messages into one batch and deletes them with batch requests
    Given SQS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And SQS is reached through a forwarder that can hold an answer
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "parallel_{{test_id}}" exists with a visibility timeout of "600s"
    And SQS queue "parallel_out_{{test_id}}" is observed
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
        CREATE RELAY notifications SCHEMA notification UNBRANCHED CAPACITY 16;
        CREATE SQS CLIENT sqs_main
        CONFIG {
          'endpoint' = '{{sqs_held_endpoint}}',
          'region' = 'us-east-1'
        };
        CREATE SQS CLIENT sqs_out
        CONFIG {
          'endpoint' = '{{sqs_endpoint}}',
          'region' = 'us-east-1'
        };
        CREATE SQS QUEUE INGESTOR sqs_parallel
        FROM sqs_main QUEUE 'parallel_{{test_id}}' MODE ACK PARALLEL MAX 10 BATCH TIMEOUT 1h ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SQS QUEUE EMITTER sqs_forward FROM notifications TO sqs_out QUEUE 'parallel_out_{{test_id}}' MODE SINGLE RETRY POLICY BACKOFF 100ms MAX 200ms ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "sqs_forward" enters stall mode
    And SQS messages are published to queue "parallel_{{test_id}}"
      """
      [{"user_id":301},{"user_id":302},{"user_id":303},{"user_id":304},{"user_id":305},{"user_id":306},{"user_id":307},{"user_id":308},{"user_id":309},{"user_id":310}]
      """
    Then SQS queue "parallel_{{test_id}}" eventually has 10 messages in flight
    And within "40s" DESCRIBE INGESTOR "sqs_parallel" on the leader node contains
      """
      mode: ACK PARALLEL MAX 10 BATCH TIMEOUT 1h ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 200ms
      """
    When emitter "sqs_forward" leaves stall mode
    Then within "40s" the observed broker receives payloads
      """
      "user_id":301
      "user_id":302
      "user_id":303
      "user_id":304
      "user_id":305
      "user_id":306
      "user_id":307
      "user_id":308
      "user_id":309
      "user_id":310
      """
    And SQS queue "parallel_{{test_id}}" eventually holds no messages
    And the SQS forwarder carried "DeleteMessageBatch" requests and no "DeleteMessage" request

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # A rejected message is made visible again at once, so the ingestor reads it again after its RETRY
  # POLICY delay. The queue's visibility timeout of ten minutes would otherwise hold it back far
  # longer than the minute this scenario waits for it.
  Scenario Outline: An SQS message its ingestor rejects returns on its RETRY POLICY instead of the queue's visibility timeout
    Given SQS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "rejected_{{test_id}}" exists with a visibility timeout of "600s"
    And SQS queue "rejected_out_{{test_id}}" is observed
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
        CREATE RELAY notifications SCHEMA notification UNBRANCHED;
        CREATE SQS CLIENT sqs_main
        CONFIG {
          'endpoint' = '{{sqs_endpoint}}',
          'region' = 'us-east-1'
        };
        CREATE SQS QUEUE INGESTOR sqs_rejecting
        FROM sqs_main QUEUE 'rejected_{{test_id}}' MODE <mode>
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SQS QUEUE EMITTER sqs_forward FROM notifications TO sqs_main QUEUE 'rejected_out_{{test_id}}' MODE SINGLE RETRY POLICY BACKOFF 100ms MAX 200ms ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    When emitter "sqs_forward" enters fault mode
    And SQS message is published to queue "rejected_{{test_id}}"
      """
      {"user_id":45}
      """
    Then the relay subscription receives a payload
      """
      "user_id":45
      """
    And within "60s" the relay subscription receives payloads
      """
      "user_id":45
      """
    When emitter "sqs_forward" leaves fault mode
    Then within "40s" the observed broker receives payloads
      """
      "user_id":45
      """
    And SQS queue "rejected_{{test_id}}" eventually holds no messages

    Examples:
      | cluster_size | replica_count | mode                                                                                           |
      | 1            | 0             | ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms                          |
      | 3            | 1             | ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms                          |
      | 1            | 0             | ACK PARALLEL MAX 10 BATCH TIMEOUT 100ms ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms |
      | 3            | 1             | ACK PARALLEL MAX 10 BATCH TIMEOUT 100ms ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms |

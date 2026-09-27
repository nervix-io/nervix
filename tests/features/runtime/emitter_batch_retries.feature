Feature: Emitter batch payloads through acknowledgements and retries
  A batch payload answers for every member it carries. One confirmation from the destination
  acknowledges every member, and a definitive rejection routes every member through ON MESSAGE
  ERROR with one shared reference. A payload whose outcome the emitter never learned is retained
  and written again byte for byte with the same members, after the payloads the destination did
  confirm, while the upstream acknowledgements of its members stay alive. Where the destination
  names individual members, as a MongoDB bulk write does, only the members it did not resolve are
  written again.

  @emitter_batch_retries
  Scenario Outline: A stalled broker's unconfirmed payloads are written again unchanged
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "retried_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "retried_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "retried_out_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event ( seq I64, error_code STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, note string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_events
        FROM KAFKA kafka_main TOPIC retried_in_{{test_id}}
          OFFSET BY CONSUMER GROUP retried_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER retried FROM events
        TO KAFKA kafka_main TOPIC retried_out_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 3s MAX 3s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 4 MAX SIZE 100B
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    Then Kafka consumer group "retried_group_{{test_id}}" eventually has 1 consumers
    # The broker confirms the first payload and stops answering: the emitter never learns whether
    # the next two landed. The retry waits three seconds, longer than the source's one-second ACK
    # TIMEOUT, so the source keeps its message only while the emitter keeps its members alive.
    When the sink of emitter "retried" stalls after resolving 1 record of its next publish
    And Kafka message is published to topic "retried_in_{{test_id}}"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},{"seq":4,"note":"d"},{"seq":5,"note":"e"},{"seq":6,"note":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},{"seq":7,"note":"g"},{"seq":8,"note":"h"}]
      """
    # Records 3 and 6 exceed MAX SIZE alone, so they divide the rest into three payloads. The retry
    # writes the two unconfirmed payloads exactly as before, rather than one payload of the four
    # records they carry, and never writes the confirmed first payload again.
    Then within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"}]
      [{"seq":4,"note":"d"},{"seq":5,"note":"e"}]
      [{"seq":7,"note":"g"},{"seq":8,"note":"h"}]
      [{"seq":4,"note":"d"},{"seq":5,"note":"e"}]
      [{"seq":7,"note":"g"},{"seq":8,"note":"h"}]
      """
    And within "10s" Kafka consumer group "retried_group_{{test_id}}" next offset for topic "retried_in_{{test_id}}" partition 0 is "at least 1"
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      "seq":3 | "error_code":"validation"
      "seq":6 | "error_code":"validation"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_retries
  Scenario Outline: A detached emitter acknowledges upstream while it retains an unconfirmed payload
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "detached_retried_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "detached_retried_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "detached_retried_out_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, note string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_events
        FROM KAFKA kafka_main TOPIC detached_retried_in_{{test_id}}
          OFFSET BY CONSUMER GROUP detached_retried_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DETACHED EMITTER detached_retried FROM events
        TO KAFKA kafka_main TOPIC detached_retried_out_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 10s MAX 10s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 4 MAX SIZE 1KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "detached_retried_group_{{test_id}}" eventually has 1 consumers
    When the sink of emitter "detached_retried" stalls after resolving 0 records of its next publish
    And Kafka message is published to topic "detached_retried_in_{{test_id}}"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"}]
      """
    # The stall holds the retry the unconfirmed payload is waiting for, so only the detached
    # acknowledgement at relay fan-out can have committed the source offset.
    When emitter "detached_retried" enters stall mode
    Then within "30s" Kafka consumer group "detached_retried_group_{{test_id}}" next offset for topic "detached_retried_in_{{test_id}}" partition 0 is "at least 1"
    When emitter "detached_retried" leaves stall mode
    Then within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"}]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_retries
  Scenario Outline: A payload the broker refuses rejects every member it carries with one shared reference
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "refused_{{test_id}}" exists with 1 partitions
    And Kafka topic "refused_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_reference STRING,
        error_code STRING,
        operation STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, note string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge refused-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_small_messages TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'message.max.bytes' = '1000'
      };
      CREATE EMITTER refused FROM events
        TO KAFKA kafka_small_messages TOPIC refused_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 2 MAX SIZE 2KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_reference = error.reference,
              error_code = error.code,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    # The producer refuses the payload of records 3 and 4, which is larger than its 1000-byte
    # message limit, while the payloads before and after it are delivered.
    And http payload is posted to host "refused-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},{"seq":4,"note":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},{"seq":5,"note":"e"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments that share one "error_reference"
      """
      "seq":3 | "error_code":"external" | "operation":"publish"
      "seq":4 | "error_code":"external" | "operation":"publish"
      """
    And within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"}]
      [{"seq":5,"note":"e"}]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_retries @mongodb_batch_retries
  Scenario Outline: A MongoDB bulk write retries only the documents it did not resolve
    Given MongoDB is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "retried_mongodb_{{test_id}}" rejecting poison actions exists
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE SCHEMA emitter_error ( error_code STRING, source_user_id I64 );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE VHOST edge mongodb-retried-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE INGESTOR http_notifications
        FROM ENDPOINT notifications_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT mongodb_client TYPE MONGODB POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = '{{mongodb_addr}}',
        'database' = 'nervix'
      };
      CREATE EMITTER to_mongodb FROM notifications
        TO MONGODB mongodb_client INSERT TO COLLECTION retried_mongodb_{{test_id}}
        VALUES {
          "mongodb_user_id" = input.user_id,
          "mongodb_action" = LOWER(input.action)
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO emitter_errors
          SET error_code = error.code,
              source_user_id = input.user_id
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    # MongoDB answers for every document of the bulk write, but the emitter learns only the answers
    # for the first two: the delivered first document and the poison document it rejected. The two
    # documents whose answers were lost are the only ones written again.
    When the sink of emitter "to_mongodb" stalls after resolving 2 records of its next publish
    And http payload is posted to host "mongodb-retried-{{test_id}}.example.com" path "/notifications"
      """
      [{"user_id":1,"action":"HEALTHY_A"},{"user_id":2,"action":"POISON"},{"user_id":3,"action":"HEALTHY_B"},{"user_id":4,"action":"HEALTHY_C"}]
      """
    Then within "10s" the relay subscription receives a payload
      """
      "error_code":"external","source_user_id":2
      """
    And the MongoDB collection eventually holds exactly these documents
      """
      {"mongodb_user_id":1,"mongodb_action":"healthy_a"}
      {"mongodb_user_id":3,"mongodb_action":"healthy_b"}
      {"mongodb_user_id":3,"mongodb_action":"healthy_b"}
      {"mongodb_user_id":4,"mongodb_action":"healthy_c"}
      {"mongodb_user_id":4,"mongodb_action":"healthy_c"}
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

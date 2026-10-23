Feature: Kafka consumer-group rewinds across a rebalance
  An acknowledged Kafka ingestor rewinds a rejected batch so that its records are delivered again.
  A rebalance can move a rejected record's partition to another member of the consumer group while
  the batch is still in flight. The ingestor keeps polling then: whichever member holds the
  partition next resumes it from the committed offset, which never passed the rejected record.
  When the group has committed nothing for the partition, the ingestor starts the partition at its
  earliest rejected record whenever the group assigns the partition to it again, until a commit
  passes that record.

  Scenario Outline: A rejected record is delivered again after its partition moves away and returns
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rewind_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "rewind_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "rewind_out_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA rewind_event ( event_id STRING );
      CREATE WIRE JSON SCHEMA rewind_wire MODE STRICT ( event_id string );
      CREATE CODEC rewind_codec FROM WIRE JSON SCHEMA rewind_wire TO SCHEMA rewind_event;
      CREATE RELAY rewind_records SCHEMA rewind_event UNBRANCHED;
      CREATE KAFKA CLIENT rewind_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR rewind_source
        FROM rewind_kafka TOPIC 'rewind_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'rewind_group_{{test_id}}'
          MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 30s ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING rewind_codec
        TO rewind_records INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER rewind_sink
        FROM rewind_records
        TO KAFKA rewind_kafka TOPIC rewind_out_{{test_id}}
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING rewind_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Kafka messages are rapidly published to topic "rewind_in_{{test_id}}"
      """
      {"event_id":"warm-1"}
      {"event_id":"warm-2"}
      """
    Then within "60s" the observed broker receives payloads
      """
      "event_id":"warm-1"
      "event_id":"warm-2"
      """
    And within "30s" Kafka consumer group "rewind_group_{{test_id}}" next offset for topic "rewind_in_{{test_id}}" partition 0 is "at least 2"
    # The ingestor's batch now holds "moved" for its 30 second window while the external member
    # takes the partition. The faulted emitter then negatively acknowledges it, so the batch is
    # rejected once the partition belongs to the external member.
    When emitter "rewind_sink" enters fault mode
    And Kafka message is published to topic "rewind_in_{{test_id}}"
      """
      {"event_id":"moved"}
      """
    And an external member joins Kafka consumer group "rewind_group_{{test_id}}" on topic "rewind_in_{{test_id}}"
    Then within "60s" the external member of Kafka consumer group "rewind_group_{{test_id}}" holds topic "rewind_in_{{test_id}}" partition 0
    # The ingestor can receive "moved" again only after the external member leaves, so this
    # rejection is of the batch that held it when the partition moved.
    And within "90s" the active session observes a server error containing
      """
      source ACK chain failed for ingestor 'rewind_source' in domain '{{domain}}'
      """
    When emitter "rewind_sink" leaves fault mode
    And the external member leaves Kafka consumer group "rewind_group_{{test_id}}"
    And Kafka message is published to topic "rewind_in_{{test_id}}"
      """
      {"event_id":"after"}
      """
    Then within "60s" the observed broker receives payloads
      """
      "event_id":"moved"
      "event_id":"after"
      """
    And within "60s" Kafka consumer group "rewind_group_{{test_id}}" next offset for topic "rewind_in_{{test_id}}" partition 0 is "at least 4"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_rejection_floor
  Scenario Outline: A rejected record of a group that committed nothing is delivered again once a broker restart returns its partition on <cluster_size> nodes
    Given a mock Kafka broker serves topic "floor_in_{{test_id}}"
    And the mock Kafka broker also serves topic "floor_out_{{test_id}}"
    And a <cluster_size> node nervix cluster is started
    And mock Kafka topic "floor_out_{{test_id}}" is observed
    And the active domain is "{{domain}}"
    # The group starts its partition where it ends, and its group session lapses six seconds after
    # its broker stops answering.
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA floor_event ( event_id STRING );
      CREATE WIRE JSON SCHEMA floor_wire MODE STRICT ( event_id string );
      CREATE CODEC floor_codec FROM WIRE JSON SCHEMA floor_wire TO SCHEMA floor_event;
      CREATE RELAY floor_records SCHEMA floor_event UNBRANCHED;
      CREATE KAFKA CLIENT floor_kafka CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}',
        'auto.offset.reset' = 'latest',
        'session.timeout.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE KAFKA TOPIC INGESTOR floor_source
        FROM floor_kafka TOPIC 'floor_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'floor_group_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING floor_codec
        TO floor_records INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER floor_sink
        FROM floor_records
        TO KAFKA floor_kafka TOPIC floor_out_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING floor_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION floor_subscription TO floor_records;
      START;
      """
    # The faulted emitter refuses every record, so the ingestor rejects each record it reads and the
    # group commits nothing.
    And emitter "floor_sink" enters fault mode
    Then within "60s" repeatedly publishing mock Kafka message to topic "floor_in_{{test_id}}" yields a relay subscription payload
      """
      {"event_id":"rejected"}
      """
    And within "60s" the active session observes a server error containing
      """
      source ACK chain failed for ingestor 'floor_source' in domain '{{domain}}'
      """
    # The outage outlasts the group session, so the group takes the partition from the ingestor and
    # assigns it again once its broker is back. Resuming from a committed offset there is none of
    # would start at the partition's end, past the rejected record.
    When the mock Kafka broker goes down
    And the mock Kafka broker comes back up after an outage of "15s"
    And emitter "floor_sink" leaves fault mode
    Then within "90s" the observed broker receives payloads
      """
      "event_id":"rejected"
      """
    When mock Kafka message is published to topic "floor_in_{{test_id}}"
      """
      {"event_id":"after"}
      """
    Then within "60s" the observed broker receives payloads
      """
      "event_id":"after"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

Feature: Pulsar ingestion
  Scenario Outline: Pulsar ingestor delivers JSON payloads to a subscribed relay
    Given Pulsar is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
        CREATE IF NOT EXISTS BRANCH by_pulsar_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_pulsar_notifications;
        CREATE PULSAR CLIENT pulsar_main
        CONFIG {
          'addr' = '{{pulsar_addr}}'
        };
        CREATE PULSAR TOPIC INGESTOR pulsar_notifications
        FROM pulsar_main TOPIC 'notifications_{{test_id}}' SUBSCRIPTION 'nervix_cucumber_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST INSTANCES <instances> MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_pulsar_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    When Pulsar message is published to topic "notifications_{{test_id}}"
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

  Scenario Outline: A Pulsar ingestor delivers every payload it reads under MODE <delivery>
    Given Pulsar is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
      CREATE PULSAR CLIENT pulsar_main
      CONFIG {
        'addr' = '{{pulsar_addr}}'
      };
      CREATE PULSAR TOPIC INGESTOR pulsar_notifications
      FROM pulsar_main TOPIC 'notifications_modes_{{test_id}}' SUBSCRIPTION 'nervix_cucumber_modes_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST MODE <mode>
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    When Pulsar message is published to topic "notifications_modes_{{test_id}}"
      """
      {"user_id":41}
      """
    And Pulsar message is published to topic "notifications_modes_{{test_id}}"
      """
      {"user_id":42}
      """
    And Pulsar message is published to topic "notifications_modes_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":41
      "user_id":42
      "user_id":43
      """

    Examples:
      | cluster_size | delivery     | mode                                                                                     |
      | 1            | NO ACK       | NO ACK                                                                                   |
      | 3            | NO ACK       | NO ACK                                                                                   |
      | 1            | ACK PARALLEL | ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |
      | 3            | ACK PARALLEL | ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |

  Scenario Outline: Pulsar ingestor reports transient source failures and recovers
    Given Pulsar is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "pulsar_notifications" enters fault mode
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
        CREATE IF NOT EXISTS BRANCH by_pulsar_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_pulsar_notifications;
        CREATE PULSAR CLIENT pulsar_main
        CONFIG {
          'addr' = '{{pulsar_addr}}'
        };
        CREATE PULSAR TOPIC INGESTOR pulsar_notifications
        FROM pulsar_main TOPIC 'notifications_reconnect_{{test_id}}' SUBSCRIPTION 'nervix_cucumber_reconnect_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_pulsar_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "pulsar_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "pulsar_notifications" leaves fault mode
    And Pulsar message is published to topic "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "pulsar_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # The ingestor reads through a scenario-owned stallable endpoint in front of the shared broker,
  # so pausing that endpoint keeps an acknowledgement from reaching the broker while every other
  # scenario's traffic flows. The emitter's stall holds the batch's acknowledgement tree until the
  # endpoint is paused.
  @pulsar_confirmed_acknowledgements
  Scenario Outline: A Pulsar acknowledgement the broker does not confirm is the ingestor's transient status and leaves the message on the subscription
    Given Pulsar is running
    And a stallable Pulsar endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "confirmed_acks_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT pulsar_ingress CONFIG {
        'addr' = '{{pulsar_stallable_addr}}'
      };
      CREATE PULSAR CLIENT pulsar_egress CONFIG {
        'addr' = '{{pulsar_addr}}'
      };
      CREATE PULSAR TOPIC INGESTOR confirmed_notifications
        FROM pulsar_ingress TOPIC 'confirmed_acks_in_{{test_id}}'
          SUBSCRIPTION 'nervix_confirmed_acks_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 120s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC EMITTER forwarded_notifications FROM notifications
        TO pulsar_egress TOPIC 'confirmed_acks_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 120s RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And emitter "forwarded_notifications" enters stall mode
    And Pulsar message is published to topic "confirmed_acks_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "30s" DESCRIBE EMITTER "forwarded_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    When the stallable endpoint "pulsar" is paused
    And emitter "forwarded_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And within "90s" DESCRIBE INGESTOR "confirmed_notifications" on the leader node contains
      """
      transient error: the Pulsar broker did not confirm an acknowledgement
      """
    And Pulsar subscription "nervix_confirmed_acks_{{test_id}}" of topic "confirmed_acks_in_{{test_id}}" holds 1 unacknowledged message
    # The acknowledgement the paused endpoint held may still reach the broker once it resumes, so
    # the first message is acknowledged or delivered again; either way only a later confirmed
    # acknowledgement clears the transient status.
    When the stallable endpoint "pulsar" is resumed
    And Pulsar message is published to topic "confirmed_acks_in_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "180s" Pulsar subscription "nervix_confirmed_acks_{{test_id}}" of topic "confirmed_acks_in_{{test_id}}" holds 0 unacknowledged messages
    And within "60s" DESCRIBE INGESTOR "confirmed_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Pausing the endpoint and resetting its connections leaves the consumer reconnecting to a broker
  # it cannot reach. The emitter's stall holds the batch until then, and its fault then makes the
  # ingestor reject the batch, whose redelivery request the consumer cannot send. The broker answers
  # no redelivery request, so a request the consumer could not send is the failure there is to see.
  @pulsar_confirmed_acknowledgements
  Scenario Outline: A Pulsar rejection whose redelivery request cannot reach the broker is the ingestor's transient status
    Given Pulsar is running
    And a stallable Pulsar endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "unsent_redeliveries_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT pulsar_ingress CONFIG {
        'addr' = '{{pulsar_stallable_addr}}'
      };
      CREATE PULSAR CLIENT pulsar_egress CONFIG {
        'addr' = '{{pulsar_addr}}'
      };
      CREATE PULSAR TOPIC INGESTOR rejected_notifications
        FROM pulsar_ingress TOPIC 'unsent_redeliveries_in_{{test_id}}'
          SUBSCRIPTION 'nervix_unsent_redeliveries_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 120s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC EMITTER forwarded_notifications FROM notifications
        TO pulsar_egress TOPIC 'unsent_redeliveries_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 120s RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And emitter "forwarded_notifications" enters stall mode
    And Pulsar message is published to topic "unsent_redeliveries_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "30s" DESCRIBE EMITTER "forwarded_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    When the stallable endpoint "pulsar" is paused
    And the stallable endpoint "pulsar" resets its connections
    And emitter "forwarded_notifications" enters fault mode
    Then within "120s" DESCRIBE INGESTOR "rejected_notifications" on the leader node contains
      """
      transient error: the Pulsar consumer did not confirm sending a redelivery request
      """
    And Pulsar subscription "nervix_unsent_redeliveries_{{test_id}}" of topic "unsent_redeliveries_in_{{test_id}}" holds 1 unacknowledged message
    When the stallable endpoint "pulsar" is resumed
    And emitter "forwarded_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And within "180s" Pulsar subscription "nervix_unsent_redeliveries_{{test_id}}" of topic "unsent_redeliveries_in_{{test_id}}" holds 0 unacknowledged messages
    And within "60s" DESCRIBE INGESTOR "rejected_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

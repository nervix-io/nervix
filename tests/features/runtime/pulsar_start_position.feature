Feature: Pulsar subscription start position
  @pulsar_start_position
  Scenario Outline: A new owned Pulsar subscription starts after the backlog when START AT LATEST is declared
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "start_latest_{{test_id}}" is observed
    When Pulsar message is published to topic "start_latest_{{test_id}}"
      """
      {"user_id":11}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT pulsar_main CONFIG {'addr' = '{{pulsar_addr}}'};
      CREATE PULSAR TOPIC INGESTOR pulsar_notifications
        FROM pulsar_main TOPIC 'start_latest_{{test_id}}'
        SUBSCRIPTION 'start_latest_{{test_id}}' OWNED BY INGESTOR START AT LATEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "30s" Pulsar subscription "start_latest_{{test_id}}" of topic "start_latest_{{test_id}}" has 1 consumer
    When Pulsar message is published to topic "start_latest_{{test_id}}"
      """
      {"user_id":22}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":22
      """

    And the relay subscription does not receive a payload containing fragments within "2s"
      """
      "user_id":11
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @pulsar_start_position
  Scenario Outline: A provisioned Pulsar cursor refuses START AT without changing its definition
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA item (user_id I64);
      CREATE WIRE JSON SCHEMA item_wire MODE STRICT (user_id integer);
      CREATE CODEC item_codec FROM WIRE JSON SCHEMA item_wire TO SCHEMA item;
      CREATE RELAY output SCHEMA item UNBRANCHED;
      CREATE PULSAR CLIENT broker CONFIG {'addr' = 'pulsar://127.0.0.1:1'};
      CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events'
        SUBSCRIPTION 'readers' PROVISIONED MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING item_codec TO output INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail
      """
      ALTER PULSAR TOPIC INGESTOR events SET START AT LATEST;
      """
    Then the last command error contains
      """
      cannot set START AT on provisioned PULSAR subscription 'readers'
      """
    When these Plexus commands are executed
      """
      SHOW CREATE INGESTOR events;
      """
    Then the last command output contains
      """
      SUBSCRIPTION 'readers' PROVISIONED
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @pulsar_start_position
  Scenario Outline: An owned Pulsar subscription replays its backlog and keeps its cursor when START AT changes
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar topic "start_earliest_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      """
    When Pulsar message is published to topic "start_earliest_{{test_id}}"
      """
      {"user_id":11}
      """
    And Pulsar ingestor "events" reads subscription "start_earliest_{{test_id}}" of topic "start_earliest_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST"
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":11
      """
    When these Plexus commands are executed on the active session
      """
      STOP;
      """
    Then within "60s" Pulsar subscription "start_earliest_{{test_id}}" of topic "start_earliest_{{test_id}}" has no consumers
    When Pulsar message is published to topic "start_earliest_{{test_id}}"
      """
      {"user_id":22}
      """
    Then within "30s" Pulsar subscription "start_earliest_{{test_id}}" of topic "start_earliest_{{test_id}}" holds 1 unacknowledged message
    When these Plexus commands are executed on the active session
      """
      ALTER PULSAR TOPIC INGESTOR events SET START AT LATEST;
      SHOW CREATE INGESTOR events;
      """
    Then the last command output contains
      """
      START AT LATEST
      """
    When these Plexus commands are executed on the active session
      """
      START;
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":22
      """

    When these Plexus commands are executed on the active session
      """
      DROP INGESTOR events;
      """
    Then within "60s" Pulsar topic "start_earliest_{{test_id}}" has no durable subscription "start_earliest_{{test_id}}"
    When Pulsar message is published to topic "start_earliest_{{test_id}}"
      """
      {"user_id":33}
      """
    And Pulsar ingestor "events" reads subscription "start_earliest_{{test_id}}" of topic "start_earliest_{{test_id}}" declared "OWNED BY INGESTOR START AT LATEST"
    Then within "30s" Pulsar subscription "start_earliest_{{test_id}}" of topic "start_earliest_{{test_id}}" has 1 consumer
    When Pulsar message is published to topic "start_earliest_{{test_id}}"
      """
      {"user_id":44}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":44
      """
    And the relay subscription does not receive a payload containing fragments within "2s"
      """
      "user_id":33
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

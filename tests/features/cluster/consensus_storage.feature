Feature: Durable consensus storage

  Scenario: A restarted node reopens its dedicated consensus database
    Given a 1 node nervix cluster is started
    When 1 domains named "dedicated_restart" are created on the leader node
    Then within "10s" node "node-1" has applied 1 domains named "dedicated_restart"
    When the cluster is restarted
    Then within "10s" node "node-1" has applied 1 domains named "dedicated_restart"

  Scenario Outline: Committed administrative changes recover atomically after storage failure
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE DOMAIN durable_preceding;
      """
    And consensus storage on the leader fails <boundary> committing domain "durable_change"
    When these Plexus commands fail with "consensus storage"
      """
      CREATE DOMAIN durable_change;
      """
    Then the storage-failed node has no published domain "durable_change"
    When the cluster is restarted
    And these Plexus commands are executed on the leader node
      """
      USE durable_preceding;
      USE durable_change;
      """
    Then the last command output contains
      """
      durable_change
      """

    Examples:
      | cluster_size | boundary |
      | 1            | before   |
      | 3            | before   |
      | 1            | after    |
      | 3            | after    |

  Scenario Outline: A Kafka source started while consensus storage is busy has room for its control calls
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And Kafka topic "slow_commits_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA slow_commit_event ( seq I64 );
      CREATE WIRE JSON SCHEMA slow_commit_wire MODE STRICT ( seq integer );
      CREATE CODEC slow_commit_codec FROM WIRE JSON SCHEMA slow_commit_wire TO SCHEMA slow_commit_event;
      CREATE RELAY slow_commit_events SCHEMA slow_commit_event UNBRANCHED;
      CREATE KAFKA CLIENT slow_commit_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR slow_commit_source
        FROM slow_commit_kafka TOPIC 'slow_commits_{{test_id}}'
        OFFSET BY DOMAIN MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING slow_commit_codec
        TO slow_commit_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION slow_commit_subscription TO slow_commit_events;
      """
    # Every storage operation then keeps its Commands working set for at least the commit delay, and
    # two sessions committing concurrently keep a second storage operation waiting for one while the
    # source opens and resumes.
    Given consensus commits on every node take "500ms"
    And 2 sessions keep creating domains named "busy" through the leader node
    When these Plexus commands are executed through the client on the leader node
      """
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "slow_commit_source" on the leader node contains
      """
      ready: true
      """
    # A control call refused for Commands memory stays the source's transient error until its next
    # accepted batch, so this holds only if every control call of the start found room.
    And within "10s" DESCRIBE INGESTOR "slow_commit_source" on the leader node contains
      """
      transient error: -
      """
    And the sessions creating domains named "busy" stop with every domain committed
    # The leader also inspects the source's partitions through control calls of its own and reports
    # a refused one to the sessions, so none of them may have been refused while the load ran.
    And the active session observes no server error containing "commands memory budget has no room" within "2s"
    When Kafka message is published to topic "slow_commits_{{test_id}}"
      """
      {"seq":1}
      """
    Then the relay subscription receives a payload
      """
      "seq":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

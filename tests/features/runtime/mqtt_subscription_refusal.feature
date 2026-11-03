Feature: Refused MQTT subscriptions never become ready through an empty retained session

  Scenario Outline: An authenticated MQTT subscription keeps retrying after refusal and recovers when permission is granted
    Given MQTT is running
    And entity gate deadline is configured as "30s"
    And a <cluster_size> node nervix cluster is started
    And MQTT client "refused_{{test_id}}" is denied subscriptions
    Then the MQTT broker retains an authenticated empty session after refusing client "probe_{{test_id}}"
    Given the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands are executed
      """
      CREATE SCHEMA item (user_id I64);
      CREATE WIRE JSON SCHEMA item_wire MODE STRICT (user_id integer);
      CREATE CODEC item_codec FROM WIRE JSON SCHEMA item_wire TO SCHEMA item;
      CREATE CODEC held_item_codec FROM WIRE JSON SCHEMA item_wire TO SCHEMA item;
      CREATE RELAY received SCHEMA item UNBRANCHED;
      CREATE MQTT CLIENT broker CONFIG {
        'addr' = '{{mqtt_auth_addr}}',
        'client_id' = 'refused_{{test_id}}',
        'username' = '{{mqtt_auth_username}}',
        'password' = '{{mqtt_auth_password}}'
      };
      CREATE MQTT TOPIC INGESTOR refused
        FROM broker TOPIC 'refused_{{test_id}}' SESSION PERSISTENT QOS 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s
        ON QUIESCE SUSPEND DECODE USING item_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      CREATE SUBSCRIPTION observed TO received;
      """
    Then within "30s" DESCRIBE INGESTOR "refused" on the leader node reports a reconnect backoff of at least "800ms"
    And within "30s" DESCRIBE INGESTOR "refused" on the leader node contains
      """
      ready: false
      """
    And within "30s" DESCRIBE INGESTOR "refused" on the leader node contains
      """
      the MQTT broker refused the subscription
      """
    When these Plexus commands begin executing in the background
      """
      ALTER MQTT TOPIC INGESTOR refused SET DECODE USING held_item_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "30s" DESCRIBE INGESTOR "refused" on the leader node contains
      """
      quiesce state: entity hold
      """
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution fails with "timed out draining domain"
    And within "30s" DESCRIBE INGESTOR "refused" on the leader node contains
      """
      codec: item_codec
      """
    And within "30s" DESCRIBE INGESTOR "refused" on the leader node reports a reconnect backoff of at least "800ms"
    And within "30s" DESCRIBE INGESTOR "refused" on the leader node contains
      """
      ready: false
      """
    And within "30s" DESCRIBE INGESTOR "refused" on the leader node contains
      """
      the MQTT broker refused the subscription
      """
    When MQTT client "refused_{{test_id}}" is allowed to subscribe
    Then within "30s" DESCRIBE INGESTOR "refused" on the leader node contains
      """
      ready: true
      """
    When MQTT QoS 1 message is published to topic "refused_{{test_id}}"
      """
      {"user_id":91}
      """
    Then the relay subscription receives a payload
      """
      "user_id":91
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

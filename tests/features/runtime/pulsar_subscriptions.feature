Feature: Pulsar subscriptions belong to their declared owner
  @pulsar_subscription_replacement
  @pulsar_source_retirement @deloxide_stress
  Scenario Outline: Replacing a Pulsar input removes the owned subscription the replacement leaves behind
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And Pulsar subscription "preceding_{{test_id}}" of topic "preceding_{{test_id}}" is provisioned
    And Pulsar subscription "<replacement_subscription>" of topic "<replacement_topic>" is provisioned
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "preceding_{{test_id}}" of topic "preceding_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST"
    Then within "30s" Pulsar subscription "preceding_{{test_id}}" of topic "preceding_{{test_id}}" has 1 consumer
    When these Plexus statements are executed
      """
      ALTER PULSAR TOPIC INGESTOR events
        SET FROM broker TOPIC '<replacement_topic>' SUBSCRIPTION '<replacement_subscription>' OWNED BY INGESTOR START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s ON QUIESCE SUSPEND;
      """
    Then within "30s" Pulsar subscription "<replacement_subscription>" of topic "<replacement_topic>" has 1 consumer
    And within "60s" Pulsar topic "preceding_{{test_id}}" has no durable subscription "preceding_{{test_id}}"

    Examples:
      | cluster_size | replacement_subscription | replacement_topic       |
      | 1            | replacement_{{test_id}}  | preceding_{{test_id}}   |
      | 3            | replacement_{{test_id}}  | preceding_{{test_id}}   |
      | 1            | preceding_{{test_id}}    | replacement_{{test_id}} |
      | 3            | preceding_{{test_id}}    | replacement_{{test_id}} |

  @pulsar_subscription_replacement
  @pulsar_source_retirement @deloxide_stress
  Scenario Outline: A Pulsar replacement reading the same subscription keeps its backlog and declared ownership
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And Pulsar subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" is provisioned
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST"
    Then within "30s" Pulsar subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" has 1 consumer
    When these Plexus statements are executed
      """
      STOP;
      """
    Then within "60s" Pulsar subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" has no consumers
    When Pulsar message is published to topic "retained_{{test_id}}"
      """
      {"user_id":44}
      """
    Then within "30s" Pulsar subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" holds 1 unacknowledged message
    When these Plexus statements are executed
      """
      CREATE CODEC equivalent_codec FROM WIRE JSON SCHEMA item_wire TO SCHEMA item;
      ALTER PULSAR TOPIC INGESTOR events SET DECODE USING equivalent_codec;
      """
    Then within "30s" Pulsar subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" holds 1 unacknowledged message
    When these Plexus statements are executed
      """
      ALTER PULSAR TOPIC INGESTOR events REPLACE ROUTE TO output
        INHERIT ALL UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """
    Then within "30s" Pulsar subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" holds 1 unacknowledged message
    When these Plexus statements are executed
      """
      CREATE PULSAR CLIENT equivalent CONFIG { 'addr' = '{{pulsar_addr}}', 'namespace' = 'public/default' };
      ALTER PULSAR TOPIC INGESTOR events
        SET FROM equivalent TOPIC 'persistent://public/default/retained_{{test_id}}' SUBSCRIPTION 'retained_{{test_id}}' <ownership> INSTANCES <instances>
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s ON QUIESCE SUSPEND;
      """
    Then within "30s" Pulsar subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" holds 1 unacknowledged message
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      START;
      """
    Then within "30s" Pulsar subscription "retained_{{test_id}}" of topic "retained_{{test_id}}" has <instances> consumers
    And the relay subscription receives a payload
      """
      "user_id":44
      """
    When Pulsar message is published to topic "retained_{{test_id}}"
      """
      {"user_id":45}
      """
    Then the relay subscription receives a payload
      """
      "user_id":45
      """

    Examples:
      | cluster_size | ownership                           | instances |
      | 1            | OWNED BY INGESTOR START AT EARLIEST | 2         |
      | 3            | OWNED BY INGESTOR START AT EARLIEST | 2         |
      | 1            | PROVISIONED                         | 1         |
      | 3            | PROVISIONED                         | 1         |

  @pulsar_subscription_ownership
  Scenario Outline: Pulsar ingestors with the same name in different domains read their own subscriptions under domain-qualified consumer names
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "first_domain"
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "first_{{test_id}}" of topic "shared_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST"
    And the active domain is "second_{{test_id}}"
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "second_{{test_id}}" of topic "shared_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST"
    Then within "30s" Pulsar subscription "first_{{test_id}}" of topic "shared_{{test_id}}" is consumed by "{{first_domain}}~events-0"
    And within "30s" Pulsar subscription "second_{{test_id}}" of topic "shared_{{test_id}}" is consumed by "second_{{test_id}}~events-0"
    And Pulsar ingestors in domains "{{first_domain}}" and "second_{{test_id}}" both deliver what topic "shared_{{test_id}}" carries

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @pulsar_subscription_ownership
  @pulsar_source_retirement @deloxide_stress
  Scenario Outline: Dropping a Pulsar ingestor removes the subscription it owns
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "owned_{{test_id}}" of topic "owned_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST"
    Then within "30s" Pulsar subscription "owned_{{test_id}}" of topic "owned_{{test_id}}" has 1 consumer
    And within "30s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      options: SUBSCRIPTION 'owned_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST INSTANCES 1
      """
    When these Plexus statements are executed
      """
      <before_drop>
      """
    Then Pulsar topic "owned_{{test_id}}" holds durable subscription "owned_{{test_id}}"
    When these Plexus statements are executed
      """
      DROP INGESTOR events;
      """
    Then within "60s" Pulsar topic "owned_{{test_id}}" has no durable subscription "owned_{{test_id}}"
    When Pulsar message is published to topic "owned_{{test_id}}"
      """
      {"user_id":42}
      """
    Then Pulsar topic "owned_{{test_id}}" has no durable subscription "owned_{{test_id}}"

    Examples:
      | cluster_size | before_drop      |
      | 1            | DESCRIBE DOMAIN; |
      | 3            | DESCRIBE DOMAIN; |
      | 1            | STOP;            |
      | 3            | STOP;            |

  @pulsar_subscription_ownership
  Scenario Outline: A provisioned Pulsar subscription outlives its dropped ingestor and keeps what arrives meanwhile
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And Pulsar subscription "provisioned_{{test_id}}" of topic "provisioned_{{test_id}}" is provisioned
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "provisioned_{{test_id}}" of topic "provisioned_{{test_id}}" declared "PROVISIONED"
    Then within "30s" Pulsar subscription "provisioned_{{test_id}}" of topic "provisioned_{{test_id}}" has 1 consumer
    And within "30s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      options: SUBSCRIPTION 'provisioned_{{test_id}}' PROVISIONED INSTANCES 1
      """
    When these Plexus statements are executed
      """
      DROP INGESTOR events;
      """
    Then within "60s" Pulsar subscription "provisioned_{{test_id}}" of topic "provisioned_{{test_id}}" has no consumers
    When Pulsar message is published to topic "provisioned_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "60s" Pulsar subscription "provisioned_{{test_id}}" of topic "provisioned_{{test_id}}" holds 1 unacknowledged message
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      """
    And Pulsar ingestor "events" reads subscription "provisioned_{{test_id}}" of topic "provisioned_{{test_id}}" declared "PROVISIONED"
    Then the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @pulsar_subscription_ownership
  Scenario Outline: A Pulsar ingestor reports its provisioned subscription missing until an operator provisions it
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      """
    And Pulsar ingestor "events" reads subscription "missing_{{test_id}}" of topic "missing_{{test_id}}" declared "PROVISIONED"
    Then within "30s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      transient error: PULSAR subscription 'missing_{{test_id}}' of topic 'persistent://public/default/missing_{{test_id}}' does not exist
      """
    And Pulsar topic "missing_{{test_id}}" has no durable subscription "missing_{{test_id}}"
    When Pulsar subscription "missing_{{test_id}}" of topic "missing_{{test_id}}" is provisioned
    And Pulsar message is published to topic "missing_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "30s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The ingestor reads through a scenario-owned stallable endpoint in front of the shared broker.
  # Stopping the domain first closes its consumers while the endpoint still forwards, so pausing
  # the endpoint leaves only the removal that dropping the ingestor starts unable to reach it. The
  # drop completes only once the removal has ended, so it runs on the session the stop opened, which
  # is attached while the removal reports its failure.
  @pulsar_subscription_ownership
  Scenario Outline: Dropping a Pulsar ingestor whose broker cannot be reached reports the subscription it could not remove
    Given Pulsar is running
    And a stallable Pulsar endpoint is configured
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_stallable_addr}}"
    And Pulsar ingestor "events" reads subscription "unreachable_{{test_id}}" of topic "unreachable_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST"
    Then within "30s" Pulsar subscription "unreachable_{{test_id}}" of topic "unreachable_{{test_id}}" has 1 consumer
    When these Plexus statements are executed
      """
      STOP;
      """
    Then within "60s" Pulsar subscription "unreachable_{{test_id}}" of topic "unreachable_{{test_id}}" has no consumers
    When the stallable endpoint "pulsar" is paused
    And these Plexus statements are executed on the active session
      """
      DROP INGESTOR events;
      """
    Then within "60s" the active session observes a server error containing
      """
      failed to remove PULSAR subscription 'unreachable_{{test_id}}' of topic 'persistent://public/default/unreachable_{{test_id}}'
      """
    And the last server error contains
      """
      failed to retire the source of ingestor 'events' in domain '{{domain}}'
      """
    And Pulsar topic "unreachable_{{test_id}}" holds durable subscription "unreachable_{{test_id}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

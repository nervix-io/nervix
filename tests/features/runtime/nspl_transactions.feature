Feature: NSPL transactions
  Scenario Outline: Implicit multi-command requests are rejected
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When this NSPL command request is executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA implicit_notification (
        user_id I64
      );
      """
    Then the last command error contains
      """
      multiple commands require BEGIN
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: COMMIT executes queued transaction commands
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA committed_notification (
        user_id I64
      );
      COMMIT
      """
    Then the last command output contains
      """
      stored model 'committed_notification'
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA committed_notification;
      """
    Then the last command output contains
      """
      CREATE SCHEMA committed_notification (user_id I64);
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: REVERT drops queued transaction commands
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      BEGIN;
      CREATE SCHEMA reverted_notification (
        user_id I64
      );
      REVERT;
      """
    Then the last command output contains
      """
      transaction reverted: dropped 1 command(s)
      """
    When these NSPL commands fail with "schema 'reverted_notification' does not exist"
      """
      SHOW CREATE SCHEMA reverted_notification;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Nested BEGIN is rejected
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands fail with "transaction is already active"
      """
      BEGIN;
      BEGIN;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mixed_schema_commit
  Scenario Outline: COMMIT atomically migrates interdependent schema and codec models
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA transaction_event (
        value STRING
      );
      CREATE STRICT WIRE JSON SCHEMA transaction_event_wire (
        value string
      );
      CREATE CODEC transaction_event_codec
        FROM WIRE JSON SCHEMA transaction_event_wire
        TO SCHEMA transaction_event;
      """
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      ALTER WIRE JSON SCHEMA transaction_event_wire
        ALTER FIELD value SET TYPE number;
      ALTER SCHEMA transaction_event
        ALTER FIELD value SET TYPE F64;
      DROP CODEC transaction_event_codec;
      CREATE CODEC transaction_event_codec
        FROM WIRE JSON SCHEMA transaction_event_wire
        TO SCHEMA transaction_event;
      COMMIT
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA transaction_event;
      """
    Then the last command output contains
      """
      CREATE SCHEMA transaction_event (value F64);
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WIRE SCHEMA transaction_event_wire;
      """
    Then the last command output contains
      """
      CREATE STRICT WIRE JSON SCHEMA transaction_event_wire (value NUMBER);
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mixed_schema_commit
  Scenario Outline: A failing mixed COMMIT applies none of its mutations
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA atomic_event (
        value STRING
      );
      """
    When these NSPL commands fail with "field `missing` does not exist"
      """
      BEGIN;
      ALTER SCHEMA atomic_event
        ADD FIELD note STRING OPTIONAL;
      CREATE SCHEMA must_not_exist (
        value STRING
      );
      ALTER SCHEMA atomic_event
        DROP FIELD missing;
      COMMIT;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA atomic_event;
      """
    Then the last command output contains
      """
      CREATE SCHEMA atomic_event (value STRING);
      """
    When these NSPL commands fail with "schema 'must_not_exist' does not exist"
      """
      SHOW CREATE SCHEMA must_not_exist;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

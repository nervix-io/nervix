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

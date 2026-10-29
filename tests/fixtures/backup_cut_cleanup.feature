# Deliberately failing bodies: run only through just test-backup-cut-cleanup.
# The supervisor requires every body to fail and every after hook to stop all nodes.
# The cut's five-minute budget keeps capture from expiring during the unchanged 60-second
# cleanup budget, even after its bounded arrival wait. It is not a cleanup deadline.
Feature: Backup cut cleanup after a failed body

  Scenario Outline: A failed Plexus backup body releases a capture gate on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Given the backup cut for domain "{{domain}}" will pause before its capture
    When these Plexus commands begin executing in the background
      """
      BACKUP DOMAIN {{domain}} TO 'unused.nvxb' TIMEOUT 5m;
      """
    Then the backup cut for domain "{{domain}}" has reached its pause
    And the backup cleanup qualification body fails deliberately

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A failed command backup body releases a lease gate on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Given the backup cut for domain "{{domain}}" will pause once it holds its lease
    When a backup command request begins in the background for cleanup qualification
      """
      BACKUP DOMAIN {{domain}} TO 'unused.nvxb' TIMEOUT 5m;
      """
    Then the backup cut for domain "{{domain}}" has reached its lease pause
    And the backup cleanup qualification body fails deliberately

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A failed CLI backup body releases a capture gate on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    When the CLI begins backing up "domain {{domain}} --timeout 5m" from node "{{leader}}" into "cleanup.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    And the backup cleanup qualification body fails deliberately

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

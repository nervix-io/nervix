@backup_delivery
Feature: Final delivery of a downloaded backup archive to standard output

  The CLI verifies a downloaded archive in a private staging file before it copies the archive to
  standard output. The complete download releases the server's copy, so a delivery that fails
  afterwards keeps the verified archive and reports it under the backup's durable execution
  reference.

  Scenario Outline: A closed standard output keeps the verified archive under its execution reference
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up the cluster from node "{{leader}}" to a standard output whose reader has closed, reporting <format>
    Then the CLI's <format> report names the undelivered archive's execution reference and kept copy
    When the kept archive is moved to "working.nvxb"
    Then backup archive "working.nvxb" is the one its execution reference recovers, with the same summary and cuts and no new capture

    Examples:
      | cluster_size | format |
      | 1            | json   |
      | 1            | text   |
      | 3            | json   |
      | 3            | text   |

  # The CLI fails while it prepares its staging directory, before it connects, so the cluster's
  # topology plays no part.
  Scenario Outline: A backup whose archive cannot be staged reports no execution reference
    Given a 1 node nervix cluster is started
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up the cluster from node "{{leader}}" to standard output without a usable staging directory, reporting <format>
    Then the CLI's <format> report of the staging failure names no execution reference

    Examples:
      | format |
      | json   |
      | text   |

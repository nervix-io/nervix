@backup_wait
Feature: Bounded multi-domain backup waits and exact recovery

  Scenario Outline: A second-domain quiesce expiry returns its failure and resumes both domains
    Given a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the backup cut for domain "{{backup_base}}_a" will pause after draining
    And the backup cut for domain "{{backup_base}}_b" will pause after draining
    When the CLI begins backing up "cluster --timeout 20s --backup-wait-timeout 1m" from node "{{leader}}" into "cut-expired.nvxb" in the background
    And backup cut "{{backup_base}}_a" is held for 12 seconds and then released
    Then the backup cut for domain "{{backup_base}}_b" has reached its pause
    And the background CLI backup finishes
    And the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And backup archive "cut-expired.nvxb" does not exist
    When the backup cut pause for domain "{{backup_base}}_b" is released
    Then node "{{leader}}" eventually reports status containing "{{backup_base}}_a status=Running pace=UNPACED"
    And node "{{leader}}" eventually reports status containing "{{backup_base}}_b status=Running pace=UNPACED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A backup execution can be recovered to another local destination
    Given a <cluster_size> node nervix cluster is started
    When a completed cluster backup is recovered with a different local destination
    Then the CLI recovers that archive to stdout with the same execution reference

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A leader change between domain cuts preserves multi-domain backup recovery
    Given a 3 node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "survivor"
    Given the backup cut for domain "{{backup_base}}_a" will pause after draining
    And the backup cut for domain "{{backup_base}}_b" will pause after draining
    When the CLI begins backing up "cluster --timeout 30s --backup-wait-timeout 1m" from node "{{leader}}" into "leader-recovery.nvxb" in the background
    And backup cut "{{backup_base}}_a" is held for 2 seconds and then released
    Then the backup cut for domain "{{backup_base}}_b" has reached its pause
    When leadership is transferred from node "{{leader}}" to node "{{survivor}}"
    Then node "{{survivor}}" eventually reports a leader other than "{{leader}}"
    And node "{{leader}}" eventually reports a leader other than "{{leader}}"
    When the backup cut pause for domain "{{backup_base}}_b" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{backup_base}}_a"
    And the CLI backup succeeded with a JSON report naming domain "{{backup_base}}_b"

  Scenario Outline: An expired reference refuses CLI recovery without starting another backup
    Given a <cluster_size> node nervix cluster is started
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster --without-state --execution-reference 00000000-0000-7000-8000-000000000001" from node "{{leader}}" into "expired.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "expired"
    And backup archive "expired.nvxb" does not exist

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A bounded CLI wait recovers the same multi-domain backup to another file
    Given a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the backup cut for domain "{{backup_base}}_a" will pause after draining
    And the backup cut for domain "{{backup_base}}_b" will pause after draining
    When the CLI begins backing up "cluster --timeout 30s --backup-wait-timeout 3s" from node "{{leader}}" into "pending.nvxb" in the background
    And backup cut "{{backup_base}}_a" is held for 2 seconds and then released
    Then the backup cut for domain "{{backup_base}}_b" has reached its pause
    And the background CLI backup finishes
    And the CLI backup wait failure exposes its execution reference for recovery
    And backup archive "pending.nvxb" does not exist
    When the CLI begins backing up "cluster --timeout 30s --execution-reference {{backup_reference}} --backup-wait-timeout 1m" from node "{{leader}}" into "recovered.nvxb" in the background
    And the backup cut pause for domain "{{backup_base}}_b" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{backup_base}}_b"
    And the recovered CLI backup keeps its execution reference and both quiesced cuts
    When the CLI backs up "cluster --timeout 31s --execution-reference {{backup_reference}}" from node "{{leader}}" into "changed.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "conflicts by content"
    And backup archive "changed.nvxb" does not exist

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A bounded native wait recovers the original multi-domain backup
    Given a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Given the backup cut for domain "{{backup_base}}_a" will pause after draining
    And the backup cut for domain "{{backup_base}}_b" will pause after draining
    When a bounded native backup wait is recovered under its original reference
    Then backup archive "native-recovered.nvxb" retains both native recovery cuts

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An NSPL cluster backup waits across independent domain cut budgets
    Given a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Given the backup cut for domain "{{backup_base}}_a" will pause after draining
    And the backup cut for domain "{{backup_base}}_b" will pause after draining
    When an NSPL cluster backup crosses two delayed cuts into "cluster-wait.nvxb"
    Then backup archive "cluster-wait.nvxb" contains both delayed quiesced domain cuts

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

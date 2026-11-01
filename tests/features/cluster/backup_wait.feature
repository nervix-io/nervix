@backup_wait @deloxide_stress_restore
Feature: Bounded multi-domain backup waits and exact recovery

  Scenario Outline: A cluster backup recovers a first-domain lease release refused by a lapsed leader lease on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    And a cluster backup recovers its first cut's refused mutation lease release
    Then backup archive "lease-release-recovered.nvxb" retains both native recovery cuts after a 0 second first cut

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The first cut's 12-second hold leaves 18 seconds of its 30-second cut budget for its lease,
  # pause, drain and capture, so only the held second cut expires, even while a leader whose raft
  # lease lapsed refuses the first cut's lease. A leader that cannot record the expired backup's
  # outcome runs the backup again under its reference, and that run's second cut expires again.
  # The CLI's wait covers both runs.
  Scenario Outline: A second-domain quiesce expiry returns its failure and resumes both domains
    Given a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the backup cut for domain "{{backup_base}}_a" will pause before its capture
    And the backup cut for domain "{{backup_base}}_b" will pause before its capture
    When the CLI begins backing up "cluster --timeout 30s --backup-wait-timeout 3m" from node "{{leader}}" into "cut-expired.nvxb" in the background
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

  # Leadership moves while the leader holds the expired backup's outcome before recording it. The
  # new leader runs the backup again under its execution reference, and that run's second cut
  # expires again. A 30-second cut budget leaves the first cut's 12-second hold room for the new
  # leader's first lease, so only the held second cut can expire.
  @long_running
  Scenario: A backup that runs again under its reference returns the second-domain quiesce expiry again
    Given a 3 node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "survivor"
    Given the backup cut for domain "{{backup_base}}_a" will pause before its capture
    And the backup cut for domain "{{backup_base}}_b" will pause before its capture
    And command finalization on node "{{leader}}" pauses before its outcome is recorded
    When the CLI begins backing up "cluster --timeout 30s" from node "{{leader}}" into "cut-expired-again.nvxb" in the background
    And backup cut "{{backup_base}}_a" is held for 12 seconds and then released
    Then the backup cut for domain "{{backup_base}}_b" has reached its pause
    And the command finalization pause on node "{{leader}}" is reached
    When leadership is transferred from node "{{leader}}" to node "{{survivor}}"
    Then node "{{survivor}}" eventually reports a leader other than "{{leader}}"
    And node "{{leader}}" eventually reports a leader other than "{{leader}}"
    When the command finalization pause on node "{{leader}}" is released
    Then the background CLI backup finishes
    And the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And backup archive "cut-expired-again.nvxb" does not exist
    When the backup cut pause for domain "{{backup_base}}_b" is released
    Then node "{{survivor}}" eventually reports status containing "{{backup_base}}_a status=Running pace=UNPACED"
    And node "{{survivor}}" eventually reports status containing "{{backup_base}}_b status=Running pace=UNPACED"

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
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "survivor"
    Given the backup cut for domain "{{backup_base}}_a" will pause before its capture
    And the backup cut for domain "{{backup_base}}_b" will pause before its capture
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
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the backup cut for domain "{{backup_base}}_a" will pause before its capture
    And the backup cut for domain "{{backup_base}}_b" will pause before its capture
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

  Scenario Outline: A bounded native wait recovers the original multi-domain backup after a <first_cut_seconds> second first cut
    Given a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Given the backup cut for domain "{{backup_base}}_a" will pause before its capture
    And the backup cut for domain "{{backup_base}}_b" will pause before its capture
    When a bounded native backup wait is recovered under its original reference after a <first_cut_seconds> second first cut
    Then backup archive "native-recovered.nvxb" retains both native recovery cuts after a <first_cut_seconds> second first cut

    Examples:
      | cluster_size | first_cut_seconds |
      | 1            | 2                 |
      | 3            | 2                 |
      | 1            | 12                |
      | 3            | 12                |

  # Each domain's cut has a budget of its own. The two holds together outlast one 40-second cut
  # budget, so a budget the two cuts shared would expire the second, and each hold leaves its cut
  # at least 18 seconds of its own budget for its lease, pause, drain and capture. A leader whose
  # raft lease lapsed refuses the cut's lease until the lease recovers, and the cut waits for it
  # inside its budget.
  Scenario Outline: A Plexus cluster backup waits across independent domain cut budgets
    Given a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Given the backup cut for domain "{{backup_base}}_a" will pause before its capture
    And the backup cut for domain "{{backup_base}}_b" will pause before its capture
    When a Plexus cluster backup with a 40 second cut budget crosses cuts delayed for 20 and 22 seconds into "cluster-wait.nvxb"
    Then backup archive "cluster-wait.nvxb" contains quiesced domain cuts delayed for 20 and 22 seconds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Leadership moves while the leader holds the backup's outcome before recording it. The new
  # leader runs the backup again under its execution reference, and the archive the client
  # receives is that run's. A cut that passed its gate unheld lasts a few seconds, whatever its
  # budget, so a 30-second cut budget leaves the 12- and 14-second holds room for the new
  # leader's first lease without hiding an unheld cut.
  @long_running @slots_6
  Scenario: A cluster backup that runs again under its reference holds each delayed cut again
    Given a 3 node nervix cluster is started
    And the active domain is saved as placeholder "backup_base"
    And the active domain is "{{backup_base}}_a"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{backup_base}}_a;
      START;
      CREATE UNPACED DOMAIN {{backup_base}}_b;
      """
    Given the active domain is "{{backup_base}}_b"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "survivor"
    Given the backup cut for domain "{{backup_base}}_a" will pause before its capture
    And the backup cut for domain "{{backup_base}}_b" will pause before its capture
    And command finalization on node "{{leader}}" pauses before its outcome is recorded
    When the CLI begins backing up "cluster --timeout 30s" from node "{{leader}}" into "cluster-wait-again.nvxb" in the background
    And backup cut "{{backup_base}}_a" is held for 12 seconds and then released
    And backup cut "{{backup_base}}_b" is held for 14 seconds and then released
    Then the command finalization pause on node "{{leader}}" is reached
    When leadership is transferred from node "{{leader}}" to node "{{survivor}}"
    Then node "{{survivor}}" eventually reports a leader other than "{{leader}}"
    And node "{{leader}}" eventually reports a leader other than "{{leader}}"
    When the command finalization pause on node "{{leader}}" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{backup_base}}_b"
    And backup archive "cluster-wait-again.nvxb" contains quiesced domain cuts delayed for 12 and 14 seconds

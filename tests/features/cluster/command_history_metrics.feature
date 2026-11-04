Feature: Command execution history observability

  Scenario Outline: The command history exposes retained outcomes on every node
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "<node>" observability metric "nervix_command_execution_records" with labels eventually equals 1
      """
      state="finished"
      """
    And node "<node>" observability metric "nervix_command_execution_records" with labels eventually equals 0
      """
      state="applying"
      """
    And node "<node>" observability metric "nervix_command_execution_records" with labels eventually equals 0
      """
      state="expired"
      """

    Examples:
      | cluster_size | node   |
      | 1            | node-1 |
      | 3            | node-1 |
      | 3            | node-2 |
      | 3            | node-3 |

  Scenario Outline: A refused command is counted once across the cluster
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this Plexus command request with an execution reference <reference> is executed on the leader node
      """
      CREATE SCHEMA refused_record (value STRING);
      """
    Then the last command error contains
      """
      <refusal>
      """
    And cluster observability metric "nervix_command_execution_admission_refusals_total" with labels eventually totals 1
      """
      reason="<reason>"
      """

    Examples:
      | cluster_size | reference               | refusal                                            | reason            |
      | 1            | created "1h" before now | has expired                                        | expired           |
      | 1            | created "10m" after now | beyond the accepted client clock-skew boundary     | future            |
      | 1            | without a creation time | does not carry a UUID version 7 creation timestamp | missing_timestamp |
      | 3            | created "1h" before now | has expired                                        | expired           |
      | 3            | created "10m" after now | beyond the accepted client clock-skew boundary     | future            |
      | 3            | without a creation time | does not carry a UUID version 7 creation timestamp | missing_timestamp |

  Scenario Outline: The history exposes an applying command until it finishes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And command execution on all nodes pauses after durable admission
    When this Plexus command request with execution reference "observed-{{test_id}}" begins executing in the background on the leader node
      """
      CREATE SCHEMA observed_record (value STRING);
      """
    Then node "<node>" observability metric "nervix_command_execution_records" with labels eventually equals 1
      """
      state="applying"
      """
    When the durable command admission pauses on all nodes are released
    Then the background command request succeeds
    And node "<node>" observability metric "nervix_command_execution_records" with labels eventually equals 0
      """
      state="applying"
      """
    And node "<node>" observability metric "nervix_command_execution_records" with labels eventually equals 2
      """
      state="finished"
      """

    Examples:
      | cluster_size | node   |
      | 1            | node-1 |
      | 3            | node-2 |

  Scenario Outline: Full history capacity is observable without evicting a retained outcome
    Given the command execution capacity is configured as 2
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this Plexus command request with execution reference "retained-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA retained_record (value STRING);
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      CREATE SCHEMA refused_record (value STRING);
      """
    Then the last command error contains
      """
      command execution capacity of 2 is full
      """
    And cluster observability metric "nervix_command_execution_admission_refusals_total" with labels eventually totals 1
      """
      reason="capacity"
      """
    When this Plexus command request with execution reference "retained-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA retained_record (value STRING);
      """
    Then the last command request succeeded
    And cluster observability metric "nervix_command_execution_admission_refusals_total" with labels eventually totals 1
      """
      reason="capacity"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A retained expiry tombstone and its refusal are observable
    Given command retry identities are valid for "10s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And command execution reference "future-{{test_id}}" is created "4m" after now
    When this Plexus command request with execution reference "future-{{test_id}}" is executed on the leader node
      """
      <command>
      """
    Then the last command request succeeded
    And within "70s" node "<node>" observability metric "nervix_command_execution_records" with labels eventually equals 1
      """
      state="expired"
      """
    When this Plexus command request with execution reference "future-{{test_id}}" is executed on the leader node
      """
      <command>
      """
    Then the last command request reports an expired execution reference
    And cluster observability metric "nervix_command_execution_admission_refusals_total" with labels eventually totals 1
      """
      reason="expired"
      """

    Examples:
      | cluster_size | node   | command                                                             |
      | 1            | node-1 | CREATE SCHEMA retained_expiry_record (value STRING);                |
      | 3            | node-2 | CREATE SCHEMA retained_expiry_record (value STRING);                |
      | 1            | node-1 | BEGIN; CREATE SCHEMA retained_expiry_record (value STRING); COMMIT; |
      | 3            | node-2 | BEGIN; CREATE SCHEMA retained_expiry_record (value STRING); COMMIT; |

  Scenario: A leadership change preserves the observed fence and does not duplicate refusal counts
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "first_leader"
    And a node other than placeholder "first_leader" is saved as placeholder "second_leader"
    And node "{{first_leader}}" observability metric "nervix_command_execution_retry_fence_timestamp_seconds" is saved as placeholder "fence"
    When this Plexus command request with an execution reference without a creation time is executed on the leader node
      """
      CREATE SCHEMA refused_record (value STRING);
      """
    Then cluster observability metric "nervix_command_execution_admission_refusals_total" with labels eventually totals 1
      """
      reason="missing_timestamp"
      """
    When leadership is transferred from node "{{first_leader}}" to node "{{second_leader}}"
    Then node "{{second_leader}}" eventually reports leader "{{second_leader}}"
    And node "{{second_leader}}" observability metric "nervix_command_execution_retry_fence_timestamp_seconds" eventually reaches saved placeholder "fence"
    And node "{{second_leader}}" observability metric "nervix_command_execution_records" with labels eventually equals 1
      """
      state="finished"
      """
    When this Plexus command request with an execution reference without a creation time is executed on the leader node
      """
      CREATE SCHEMA refused_record (value STRING);
      """
    Then cluster observability metric "nervix_command_execution_admission_refusals_total" with labels eventually totals 2
      """
      reason="missing_timestamp"
      """

Feature: Altering relays
  @alter_relay_schema_under_domain_pause
  Scenario Outline: Changing a running relay schema uses domain pause
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event_v1 ( seq I64 );
      CREATE SCHEMA event_v2 ( seq I64, note STRING OPTIONAL );
      CREATE RELAY events SCHEMA event_v1 UNBRANCHED;
      START;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER RELAY events SET SCHEMA event_v2;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE RELAY events;
      """
    Then the last command output contains
      """
      CREATE RELAY events SCHEMA event_v2 UNBRANCHED CAPACITY 1;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_relay_show_create_roundtrip
  Scenario Outline: Full ALTER RELAY applies operations in order and SHOW CREATE renders the result
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event_v1 ( seq I64 );
      CREATE SCHEMA event_v2 ( tenant STRING, seq I64 );
      CREATE SCHEMA tenant_branch_schema ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch_schema TTL 5m;
      CREATE RELAY events SCHEMA event_v1 UNBRANCHED;
      ALTER RELAY events
        SET CAPACITY 8,
        SET SCHEMA event_v2,
        SET BRANCHED BY by_tenant,
        SET MATERIALIZED STATE LAST BY TIMESTAMP;
      SHOW CREATE RELAY events;
      """
    Then the last command output contains
      """
      CREATE RELAY events SCHEMA event_v2 BRANCHED BY by_tenant CAPACITY 8 WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

Feature: Fields named like keywords inside expressions

  An expression reads a statement keyword as an ordinary name wherever it can write a name, and it
  ends where it cannot continue, so a processor can read and write fields named `to`, `on` or `by`
  in any expression it holds. A name spelled like a word the expression grammar reserves is written
  between backticks. SHOW CREATE writes NSPL that creates the same processor again.

  @keyword_named_fields_show_create_roundtrip
  Scenario Outline: SHOW CREATE writes fields named like keywords as NSPL that recreates them
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA trip ( id I64, to I64, on I64, by I64, end I64, path STRING );
      CREATE RELAY trips SCHEMA trip UNBRANCHED;
      CREATE RELAY routed SCHEMA trip UNBRANCHED;
      CREATE JUNCTION route_trips
        FROM trips WHERE input.to > 0 AND input.end > input.on
        UNBRANCHED
        TO routed
          SET id = input.id, to = input.to, on = to + 1, by = input.by, `end` = input.end,
            path = input.path
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CREATE JUNCTION route_trips;
      """
    Then the last command output contains
      """
      CREATE ATTACHED JUNCTION route_trips
        FROM trips WHERE input.to > 0 AND input.end > input.on
        UNBRANCHED
        TO routed
          SET id = input.id,
              to = input.to,
              on = to + 1,
              by = input.by,
              `end` = input.end,
              path = input.path
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION route_trips SET FILTER WHERE input.by > input.on, SET DETACHED;
      SHOW CREATE JUNCTION route_trips;
      """
    Then the last command output contains
      """
      CREATE DETACHED JUNCTION route_trips
        FROM trips WHERE input.to > 0 AND input.end > input.on
        FILTER WHERE input.by > input.on
        UNBRANCHED
      """
    When these NSPL commands are executed on the leader node
      """
      DROP JUNCTION route_trips;
      CREATE DETACHED JUNCTION route_trips
        FROM trips WHERE input.to > 0 AND input.end > input.on
        FILTER WHERE input.by > input.on
        UNBRANCHED
        TO routed
          SET id = input.id,
              to = input.to,
              on = to + 1,
              by = input.by,
              `end` = input.end,
              path = input.path
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      SHOW CREATE JUNCTION route_trips;
      """
    Then the last command output contains
      """
      TO routed
          SET id = input.id,
              to = input.to,
              on = to + 1,
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

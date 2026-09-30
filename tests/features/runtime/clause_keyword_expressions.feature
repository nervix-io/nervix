Feature: Clause keywords written inside expressions

  A statement reads each expression it embeds up to the keyword that begins its next clause. A
  keyword written as the name of a call belongs to the expression instead, so a processor may call
  `max`, `right` or `replace` in any expression, and SHOW CREATE writes NSPL that creates the same
  processor again.

  @clause_keyword_calls_show_create_roundtrip
  Scenario Outline: SHOW CREATE writes parenthesized calls named like clause keywords as NSPL that recreates them
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading ( id I64, name STRING, readings <readings_type> );
      CREATE RELAY sensors SCHEMA reading UNBRANCHED;
      CREATE RELAY labels SCHEMA reading UNBRANCHED;
      CREATE RELAY peaks SCHEMA reading UNBRANCHED;
      CREATE RELAY matched SCHEMA reading UNBRANCHED;
      CREATE JUNCTION peak_readings
        FROM sensors WHERE (max(input.readings) > 10)
        UNBRANCHED
        TO peaks INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE CORRELATOR suffix_matches
        LEFT FROM sensors WHERE (right(left.name, 2) = 'ab')
        RIGHT FROM labels
        CORRELATE WHERE left.id = right.id
        MATCH EARLIEST
        MAX TIME 5s
        ON CORRELATION TIMEOUT DROP, DROP
        UNBRANCHED
        TO matched SET id = left.id, name = right.name, readings = left.readings
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CREATE JUNCTION peak_readings;
      """
    Then the last command output contains
      """
      CREATE ATTACHED JUNCTION peak_readings
        FROM sensors WHERE max(input.readings) > 10
        UNBRANCHED
        TO peaks
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CORRELATOR suffix_matches;
      """
    Then the last command output contains
      """
      CREATE ATTACHED CORRELATOR suffix_matches
        LEFT FROM sensors WHERE right(left.name, 2) = 'ab'
        RIGHT FROM labels
        CORRELATE WHERE left.id = right.id
        MATCH EARLIEST
        MAX TIME 5s
        ON CORRELATION TIMEOUT DROP, DROP
        UNBRANCHED
        TO matched
          SET id = left.id,
              name = right.name,
              readings = left.readings
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      DROP JUNCTION peak_readings;
      DROP CORRELATOR suffix_matches;
      CREATE ATTACHED JUNCTION peak_readings
        FROM sensors WHERE max(input.readings) > 10
        UNBRANCHED
        TO peaks
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE ATTACHED CORRELATOR suffix_matches
        LEFT FROM sensors WHERE right(left.name, 2) = 'ab'
        RIGHT FROM labels
        CORRELATE WHERE left.id = right.id
        MATCH EARLIEST
        MAX TIME 5s
        ON CORRELATION TIMEOUT DROP, DROP
        UNBRANCHED
        TO matched
          SET id = left.id,
              name = right.name,
              readings = left.readings
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      SHOW CREATE CORRELATOR suffix_matches;
      """
    Then the last command output contains
      """
      LEFT FROM sensors WHERE right(left.name, 2) = 'ab'
      """

    Examples:
      | cluster_size | readings_type |
      | 1            | VEC<I64>      |
      | 3            | VEC<I64>      |

  @clause_keyword_calls_in_processor_regions
  Scenario Outline: Calls named like clause keywords are read wherever a processor holds an expression
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading ( id I64, name STRING, readings <readings_type> );
      CREATE RELAY sensors SCHEMA reading UNBRANCHED;
      CREATE RELAY peaks SCHEMA reading UNBRANCHED;
      CREATE RELAY distinct_readings SCHEMA reading UNBRANCHED;
      CREATE RELAY ordered_readings SCHEMA reading UNBRANCHED;
      CREATE JUNCTION peak_readings
        FROM sensors
        FILTER WHERE max(input.readings) > 10
        UNBRANCHED
        TO peaks INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR distinct_peaks
        FROM sensors
        DEDUPLICATE ON coalesce(max(input.readings), 0)
        MAX TIME 10m
        UNBRANCHED
        TO distinct_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE REORDERER peaks_in_order
        FROM sensors
        BY coalesce(max(input.readings), 0)
        MAX TIME 10s
        UNBRANCHED
        TO ordered_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CREATE DEDUPLICATOR distinct_peaks;
      """
    Then the last command output contains
      """
      CREATE ATTACHED DEDUPLICATOR distinct_peaks
        FROM sensors
        DEDUPLICATE ON coalesce(max(input.readings), 0)
        MAX TIME 10m
        UNBRANCHED
        TO distinct_readings
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE REORDERER peaks_in_order;
      """
    Then the last command output contains
      """
      CREATE ATTACHED REORDERER peaks_in_order
        FROM sensors
        BY coalesce(max(input.readings), 0)
        MAX TIME 10s
        UNBRANCHED
        TO ordered_readings
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION peak_readings
        SET FILTER WHERE concat(input.name, replace(input.name, 'a', 'b')) != '',
        SET DETACHED;
      SHOW CREATE JUNCTION peak_readings;
      """
    Then the last command output contains
      """
      CREATE DETACHED JUNCTION peak_readings
        FROM sensors
        FILTER WHERE concat(input.name, replace(input.name, 'a', 'b')) != ''
        UNBRANCHED
        TO peaks
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | readings_type |
      | 1            | VEC<I64>      |
      | 3            | VEC<I64>      |

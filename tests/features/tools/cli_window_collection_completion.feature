@window_collection_completion
Feature: CLI completion offers the collection aggregates a window route can call

  A window processor's route returns the values its window retains as one list through
  COLLECT_VEC and COLLECT_ARRAY. Completing a function name in the value a route's SET assigns
  offers each of them where the window's bound admits it, by the rule the window applies when it is
  created: COLLECT_ARRAY under WIDTH n MESSAGES with no duration, whose count is the array's
  length, and COLLECT_VEC under a message WIDTH or a MAX STATE SIZE, which bounds the rows it
  returns. The ordinary functions stay offered beside them, so a list function can still take a
  collection. Inside an aggregate's argument no aggregate is offered, because aggregates do not
  nest. A route's WHERE reads the finished output and the routes of other processors have no
  window, so neither offers them.

  Scenario Outline: Completion offers the collection aggregates the window's bound admits on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA reading ( device STRING, cpu F64 );
      CREATE SCHEMA history ( device STRING, cpu_values <array_type> );
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY histories SCHEMA history UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    And the CLI completion of the word ending each input on node "{{leader}}" offers exactly
      | input                                                                                                                                                                           | offers                     |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET cpu_values = COLLECT_                                                | COLLECT_ARRAY, COLLECT_VEC |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET cpu_values = collect_                                                | COLLECT_ARRAY, COLLECT_VEC |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 10s DURATION STEP 1s DURATION UNBRANCHED TO histories SET cpu_values = COLLECT_                                             |                            |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 10s DURATION STEP 1s DURATION MAX STATE SIZE 1MiB UNBRANCHED TO histories SET cpu_values = COLLECT_                         | COLLECT_VEC                |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES 10s DURATION STEP 1 MESSAGES UNBRANCHED TO histories SET cpu_values = COLLECT_                                   | COLLECT_VEC                |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET device = LAST(input.device), cpu_values = list_flatten(COLLECT_      | COLLECT_ARRAY, COLLECT_VEC |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET cpu_values = list_med                                                | list_median                |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET cpu_values = COLLECT_ARRAY(COLLECT_                                  |                            |
      | CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET cpu_values = COLLECT_ARRAY(input.cpu) WHERE output.device = COLLECT_ |                            |
      | CREATE JUNCTION copy FROM readings UNBRANCHED TO histories SET cpu_values = COLLECT_                                                                                            |                            |
    When the CLI suggests for "CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET device = LAST(input.device), cpu_values = COLLECT_A|" on node "{{leader}}"
    Then the CLI offers exactly "COLLECT_ARRAY"
    And the CLI suggestion "COLLECT_ARRAY" applied to "CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET device = LAST(input.device), cpu_values = COLLECT_A|" yields "CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET device = LAST(input.device), cpu_values = COLLECT_ARRAY"
    When these Plexus commands are executed on the leader node
      """
      CREATE WINDOW PROCESSOR history FROM readings WIDTH 3 MESSAGES STEP 1 MESSAGES UNBRANCHED TO histories SET device = LAST(input.device), cpu_values = COLLECT_ARRAY(input.cpu) ON MESSAGE ERROR LOG;
      """
    And the CLI executes "SHOW CREATE WINDOW PROCESSOR history;" on node "{{leader}}"
    Then the CLI output contains "collect_array(input.cpu)"

    Examples:
      | cluster_size | array_type    |
      | 1            | ARRAY<F64, 3> |
      | 3            | ARRAY<F64, 3> |

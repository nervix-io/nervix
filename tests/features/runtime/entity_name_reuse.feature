Feature: Entity name reuse across kinds
  Scenario: Different entity kinds may share the same identifier
    Given a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA shared_name (
        user_id I64
      );

      CREATE CLIENT shared_name
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
      """
    And these NSPL commands are executed
      """
      SHOW CREATE SCHEMA shared_name;
      """
    Then the last command output contains
      """
      CREATE SCHEMA shared_name (user_id I64);
      """
    When these NSPL commands are executed
      """
      SHOW CREATE CLIENT shared_name;
      """
    Then the last command output contains
      """
      CREATE CLIENT shared_name TYPE KAFKA CONFIG {'bootstrap.servers' = '127.0.0.1:9092'};
      """

Feature: Client wire full-process restart
  @client_wire_process_restart @exclusive
  Scenario: A SIGKILL restart preserves a command admitted before the crash
    Given a nervix-server process is started
    And the server process is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA durable_after_kill (
        sequence I64
      );
      """
    When the server process receives SIGKILL
    Then the server process exits because of SIGKILL
    When the server process is restarted
    And these NSPL commands are executed on the server process
      """
      SHOW CREATE SCHEMA durable_after_kill;
      """
    Then the last command output contains
      """
      CREATE SCHEMA durable_after_kill (
        sequence I64
      );
      """

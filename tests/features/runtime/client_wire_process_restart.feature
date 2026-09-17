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

  @client_wire_inactivity_restart @exclusive
  Scenario: Physical inactivity while every node is stopped expires an open transaction
    Given a nervix-server process is started with transaction idle timeout "1s" and tombstone retention "5s"
    And the server process is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100000h;
      START AT '2000-01-01T00:00:00Z' TIME RATE 1000000.0;
      """
    When an open transaction is held on the server process as placeholder "transaction_id"
    And the server process receives SIGKILL
    Then the server process exits because of SIGKILL
    When physical time passes for "2s"
    And the server process is restarted
    Then server process transaction "{{transaction_id}}" eventually has state "EXPIRED"

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

  @client_wire15 @exclusive
  Scenario: Every killed process recovers retained commands and expires an overdue open transaction
    Given a 3 node nervix-server process cluster is started with transaction idle timeout "10s" and tombstone retention "5m"
    And the active domain is "{{domain}}"
    And the server process cluster is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this NSPL command request with execution reference "process-retained-{{test_id}}" is executed on the server process cluster
      """
      CREATE SCHEMA process_retained (value STRING);
      """
    And an open transaction is held on the server process cluster as placeholder "overdue_transaction"
    And the held server process cluster transaction queues these NSPL commands
      """
      CREATE SCHEMA process_pending (value STRING);
      """
    Then server process cluster transaction "{{overdue_transaction}}" eventually has state "OPEN"
    When all server processes receive SIGKILL
    And physical time passes for "11s"
    And all server processes restart from their existing databases
    Then server process cluster transaction "{{overdue_transaction}}" eventually has state "EXPIRED"
    And the server process cluster has no schema "process_pending"
    When this NSPL command request with execution reference "process-retained-{{test_id}}" is executed on the server process cluster
      """
      CREATE SCHEMA process_retained (value STRING);
      """
    Then the server process cluster has schema "process_retained"

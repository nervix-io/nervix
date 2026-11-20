@request_progress
Feature: A client keeps waiting for a request the server reports progress on
  While the server still serves a request a client waits on, it sends that client keepalive
  replies saying the request is in progress. A client's request and retry timeouts measure only
  how long nothing arrives for a request, so a command that takes longer than them is still
  answered.

  @request_progress_outlasts_client_timeouts
  Scenario: A client keeps waiting for a commit that outlasts its request and retry timeouts while the server reports progress
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "operator" is connected to node "node-1" with request and retry timeouts of "2s"
    And client "observer" is connected to node "node-1"
    When client "operator" executes these Plexus commands
      """
      BEGIN;
      CREATE SCHEMA progress_record (value STRING);
      """
    Given transaction commit on node "node-1" pauses after 1 statement
    When client "operator" begins executing these Plexus commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 1 statement is reached
    # The held commit outlasts both of the client's two-second timeouts several times over; the
    # server's progress replies keep the client waiting for its answer.
    And the background Plexus execution is still running after "8s"
    When the transaction commit pause on node "node-1" after 1 statement is released
    Then within "60s" the background Plexus execution reaches a successful outcome
    When client "observer" executes these Plexus commands
      """
      SHOW CREATE SCHEMA progress_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA progress_record (
        value STRING
      );
      """

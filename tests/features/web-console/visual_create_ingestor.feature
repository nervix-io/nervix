Feature: Web console visual ingestor creation

  @visual_ingestor_endpoint
  Scenario Outline: A visually created endpoint ingestor delivers to its selected relay
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_ingest_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_ingest_wire MODE STRICT (message string);
      CREATE CODEC visual_ingest_codec FROM WIRE JSON SCHEMA visual_ingest_wire TO SCHEMA visual_ingest_event;
      CREATE RELAY visual_ingest_relay SCHEMA visual_ingest_event UNBRANCHED;
      CREATE VHOST visual_ingest_host ingest-{{test_id}}.example.com;
      CREATE ENDPOINT visual_ingest_endpoint ON visual_ingest_host PATH '/visual-ingest' TYPE HTTP;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "visual_ingest_source"
    And selector ".create-ingestor-source [data-source='endpoint']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "visual_ingest_endpoint"
    Then selector ".create-ingestor-source-ref [data-value='visual_ingest_endpoint']" exists
    When selector ".create-ingestor-source-ref [data-value='visual_ingest_endpoint']" is clicked
    And selector ".create-ingestor-quiesce [data-quiesce='buffer']" is clicked
    And selector ".create-ingestor-buffer-size" is filled with "1MiB"
    And selector ".create-ingestor-codec .create-choice-search" is filled with "visual_ingest_codec"
    Then selector ".create-ingestor-codec [data-value='visual_ingest_codec']" exists
    When selector ".create-ingestor-codec [data-value='visual_ingest_codec']" is clicked
    And selector ".create-ingestor-timestamp [data-timestamp='now']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branching [data-branch='unbranched']" is clicked
    And selector ".create-ingestor-route .create-ingestor-relay .create-choice-search" is filled with "visual_ingest_relay"
    Then selector ".create-ingestor-route .create-ingestor-relay [data-value='visual_ingest_relay']" exists
    When selector ".create-ingestor-route .create-ingestor-relay [data-value='visual_ingest_relay']" is clicked
    And selector ".create-ingestor-route .create-ingestor-inherit [data-inherit='all']" is clicked
    And selector ".create-ingestor-route .create-ingestor-flush [data-flush='immediate']" is clicked
    And selector ".create-ingestor-route .create-ingestor-message-error [data-error='log']" is clicked
    And selector ".create-ingestor-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE INGESTOR visual_ingest_source FROM ENDPOINT visual_ingest_endpoint"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_ingest_subscription TO visual_ingest_relay;
      START;
      """
    And http payload is posted to host "ingest-{{test_id}}.example.com" path "/visual-ingest"
      """
      {"message":"created visually"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "message":"created visually"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_ingestor_incomplete
  Scenario: An incomplete ingestor stays editable and reports the missing source
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "incomplete_source"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "source type"
    And selector ".create-name" has value "incomplete_source"
    When selector ".create-ingestor-source [data-source='endpoint']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "source client or endpoint"

  @visual_ingestor_scope
  Scenario: Changing the ingestor draft domain invalidates its selected source and codec
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_other;
      CREATE SCHEMA visual_scope_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_scope_wire MODE STRICT (message string);
      CREATE CODEC visual_scope_codec FROM WIRE JSON SCHEMA visual_scope_wire TO SCHEMA visual_scope_event;
      CREATE VHOST visual_scope_host scope-{{test_id}}.example.com;
      CREATE ENDPOINT visual_scope_endpoint ON visual_scope_host PATH '/scope' TYPE HTTP;
      """
    And the web console is opened on the leader node
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "scoped_source"
    And selector ".create-ingestor-source [data-source='endpoint']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "visual_scope_endpoint"
    Then selector ".create-ingestor-source-ref [data-value='visual_scope_endpoint']" exists
    When selector ".create-ingestor-source-ref [data-value='visual_scope_endpoint']" is clicked
    And selector ".create-ingestor-codec .create-choice-search" is filled with "visual_scope_codec"
    Then selector ".create-ingestor-codec [data-value='visual_scope_codec']" exists
    When selector ".create-ingestor-codec [data-value='visual_scope_codec']" is clicked
    And selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    And selector ".create-scope-change" exists
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{domain}}_other"
    And selector ".create-reference-invalid" exists
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "changed context"

  @visual_ingestor_transaction
  Scenario: An ingestor selects a codec staged earlier in its transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_tx_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_tx_wire MODE STRICT (message string);
      CREATE RELAY staged_ingest_relay SCHEMA visual_tx_event UNBRANCHED;
      CREATE VHOST visual_tx_host tx-{{test_id}}.example.com;
      CREATE ENDPOINT visual_tx_endpoint ON visual_tx_host PATH '/tx' TYPE HTTP;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".prompt-row input" is filled with "CREATE CODEC staged_ingest_codec FROM WIRE JSON SCHEMA visual_tx_wire TO SCHEMA visual_tx_event;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "visual_tx_source"
    And selector ".create-ingestor-source [data-source='endpoint']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "visual_tx_endpoint"
    Then selector ".create-ingestor-source-ref [data-value='visual_tx_endpoint']" exists
    When selector ".create-ingestor-source-ref [data-value='visual_tx_endpoint']" is clicked
    And selector ".create-ingestor-quiesce [data-quiesce='buffer']" is clicked
    And selector ".create-ingestor-buffer-size" is filled with "1MiB"
    And selector ".create-ingestor-codec .create-choice-search" is filled with "staged_ingest_codec"
    Then selector ".create-ingestor-codec [data-value='staged_ingest_codec']" exists
    When selector ".create-ingestor-codec [data-value='staged_ingest_codec']" is clicked
    And selector ".create-ingestor-timestamp [data-timestamp='now']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branching [data-branch='unbranched']" is clicked
    And selector ".create-ingestor-route .create-ingestor-relay .create-choice-search" is filled with "staged_ingest_relay"
    Then selector ".create-ingestor-route .create-ingestor-relay [data-value='staged_ingest_relay']" exists
    When selector ".create-ingestor-route .create-ingestor-relay [data-value='staged_ingest_relay']" is clicked
    And selector ".create-ingestor-route .create-ingestor-inherit [data-inherit='all']" is clicked
    And selector ".create-ingestor-route .create-ingestor-flush [data-flush='immediate']" is clicked
    And selector ".create-ingestor-route .create-ingestor-message-error [data-error='log']" is clicked
    And selector ".create-ingestor-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE INGESTOR visual_tx_source FROM ENDPOINT visual_tx_endpoint"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    And selector ".create-status" contains "position 2"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "COMPLETE"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector ".prompt-row input" is filled with "SHOW CREATE INGESTOR visual_tx_source;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE INGESTOR visual_tx_source"

  @visual_ingestor_reconnect
  Scenario: An ingestor draft submits after the console follows a new leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_reconnect_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_reconnect_wire MODE STRICT (message string);
      CREATE CODEC visual_reconnect_codec FROM WIRE JSON SCHEMA visual_reconnect_wire TO SCHEMA visual_reconnect_event;
      CREATE RELAY visual_reconnect_relay SCHEMA visual_reconnect_event UNBRANCHED;
      CREATE VHOST visual_reconnect_host reconnect-{{test_id}}.example.com;
      CREATE ENDPOINT visual_reconnect_endpoint ON visual_reconnect_host PATH '/reconnect' TYPE HTTP;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "visual_reconnected_source"
    And selector ".create-ingestor-source [data-source='endpoint']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "visual_reconnect_endpoint"
    Then selector ".create-ingestor-source-ref [data-value='visual_reconnect_endpoint']" exists
    When selector ".create-ingestor-source-ref [data-value='visual_reconnect_endpoint']" is clicked
    And selector ".create-ingestor-quiesce [data-quiesce='buffer']" is clicked
    And selector ".create-ingestor-buffer-size" is filled with "1MiB"
    And selector ".create-ingestor-codec .create-choice-search" is filled with "visual_reconnect_codec"
    Then selector ".create-ingestor-codec [data-value='visual_reconnect_codec']" exists
    When selector ".create-ingestor-codec [data-value='visual_reconnect_codec']" is clicked
    And selector ".create-ingestor-timestamp [data-timestamp='now']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branching [data-branch='unbranched']" is clicked
    And selector ".create-ingestor-route .create-ingestor-relay .create-choice-search" is filled with "visual_reconnect_relay"
    Then selector ".create-ingestor-route .create-ingestor-relay [data-value='visual_reconnect_relay']" exists
    When selector ".create-ingestor-route .create-ingestor-relay [data-value='visual_reconnect_relay']" is clicked
    And selector ".create-ingestor-route .create-ingestor-inherit [data-inherit='all']" is clicked
    And selector ".create-ingestor-route .create-ingestor-flush [data-flush='immediate']" is clicked
    And selector ".create-ingestor-route .create-ingestor-message-error [data-error='log']" is clicked
    And selector ".create-ingestor-general-error [data-error='log']" is clicked
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".create-preview" contains "CREATE INGESTOR visual_reconnected_source"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE INGESTOR visual_reconnected_source;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE INGESTOR visual_reconnected_source"

  @visual_ingestor_branch
  Scenario: A visual route constructs a branch and leaks one selected sensitive field explicitly
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_branch_input (tenant STRING, secret STRING SENSITIVE);
      CREATE SCHEMA visual_branch_output (tenant STRING, secret STRING);
      CREATE SCHEMA visual_branch_key (tenant STRING);
      CREATE SCHEMA visual_branch_error (reason STRING OPTIONAL);
      CREATE BRANCH visual_branch SCHEMA visual_branch_key TTL 5m;
      CREATE WIRE JSON SCHEMA visual_branch_wire MODE STRICT (tenant string, secret string);
      CREATE CODEC visual_branch_codec FROM WIRE JSON SCHEMA visual_branch_wire TO SCHEMA visual_branch_input;
      CREATE RELAY visual_branch_relay SCHEMA visual_branch_output BRANCHED BY visual_branch;
      CREATE RELAY visual_branch_error_relay SCHEMA visual_branch_error UNBRANCHED;
      CREATE VHOST visual_branch_host branch-{{test_id}}.example.com;
      CREATE ENDPOINT visual_branch_endpoint ON visual_branch_host PATH '/branch' TYPE HTTP;
      """
    And the web console is opened on the leader node
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "visual_branch_source"
    And selector ".create-ingestor-source [data-source='endpoint']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "visual_branch_endpoint"
    Then selector ".create-ingestor-source-ref [data-value='visual_branch_endpoint']" exists
    When selector ".create-ingestor-source-ref [data-value='visual_branch_endpoint']" is clicked
    And selector ".create-ingestor-quiesce [data-quiesce='buffer']" is clicked
    And selector ".create-ingestor-buffer-size" is filled with "1MiB"
    And selector ".create-ingestor-codec .create-choice-search" is filled with "visual_branch_codec"
    Then selector ".create-ingestor-codec [data-value='visual_branch_codec']" exists
    When selector ".create-ingestor-codec [data-value='visual_branch_codec']" is clicked
    And selector ".create-ingestor-timestamp [data-timestamp='now']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branching [data-branch='named']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branch .create-choice-search" is filled with "visual_branch"
    Then selector ".create-ingestor-route .create-ingestor-branch [data-value='visual_branch']" exists
    When selector ".create-ingestor-route .create-ingestor-branch [data-value='visual_branch']" is clicked
    Then selector ".create-ingestor-branch-fields [data-value='tenant']" exists
    When selector ".create-ingestor-branch-fields [data-value='tenant']" is clicked
    And selector ".create-ingestor-branch-assignments .create-ingestor-assignment-expression" is filled with "message.tenant"
    And selector ".create-ingestor-route .create-ingestor-relay .create-choice-search" is filled with "visual_branch_relay"
    Then selector ".create-ingestor-route .create-ingestor-relay [data-value='visual_branch_relay']" exists
    When selector ".create-ingestor-route .create-ingestor-relay [data-value='visual_branch_relay']" is clicked
    And selector ".create-ingestor-route .create-ingestor-inherit [data-inherit='fields']" is clicked
    Then selector ".create-ingestor-input-fields [data-value='tenant']" exists
    When selector ".create-ingestor-input-fields [data-value='tenant']" is clicked
    And selector ".create-ingestor-input-fields [data-value='secret']" is clicked
    And selector ".create-ingestor-inherited-field[data-field='secret'] .create-ingestor-leak" is clicked
    And selector ".create-ingestor-route .create-ingestor-flush [data-flush='immediate']" is clicked
    And selector ".create-ingestor-route .create-ingestor-message-error [data-error='send']" is clicked
    And selector ".create-ingestor-error-relay .create-choice-search" is filled with "visual_branch_error_relay"
    Then selector ".create-ingestor-error-relay [data-value='visual_branch_error_relay']" exists
    When selector ".create-ingestor-error-relay [data-value='visual_branch_error_relay']" is clicked
    Then selector ".create-ingestor-error-fields [data-value='reason']" exists
    When selector ".create-ingestor-error-fields [data-value='reason']" is clicked
    And selector ".create-ingestor-error-assignments .create-ingestor-assignment-expression" is filled with "error.message"
    And selector ".create-ingestor-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "LEAK SENSITIVE"
    And selector ".create-preview" contains "ON MESSAGE ERROR SEND TO visual_branch_error_relay"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_branch_subscription TO visual_branch_relay;
      START;
      """
    And http payload is posted to host "branch-{{test_id}}.example.com" path "/branch"
      """
      {"tenant":"alpha","secret":"visible"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | payload={"secret":"visible","tenant":"alpha"}
      """

  @visual_ingestor_client
  Scenario Outline: A visually created HTTP client ingestor polls and delivers decoded records
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_http_event (user_id I64, payload STRING);
      CREATE CODEC visual_http_codec FROM JSON TO SCHEMA visual_http_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '{user_id, payload: "aligned"}';
      CREATE RELAY visual_http_relay SCHEMA visual_http_event UNBRANCHED;
      CREATE CLIENT visual_http_client TYPE HTTP CONFIG {
        'endpoint' = '{{mock_http_addr}}/http/{{test_id}}',
        'method' = 'GET',
        'timeout_ms' = 5000
      };
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "visual_http_source"
    And selector ".create-ingestor-source [data-source='http']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "visual_http_client"
    Then selector ".create-ingestor-source-ref [data-value='visual_http_client']" exists
    When selector ".create-ingestor-source-ref [data-value='visual_http_client']" is clicked
    And selector ".create-ingestor-every" is filled with "1s"
    And selector ".create-ingestor-quiesce [data-quiesce='suspend']" is clicked
    And selector ".create-ingestor-codec .create-choice-search" is filled with "visual_http_codec"
    Then selector ".create-ingestor-codec [data-value='visual_http_codec']" exists
    When selector ".create-ingestor-codec [data-value='visual_http_codec']" is clicked
    And selector ".create-ingestor-timestamp [data-timestamp='now']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branching [data-branch='unbranched']" is clicked
    And selector ".create-ingestor-route .create-ingestor-relay .create-choice-search" is filled with "visual_http_relay"
    Then selector ".create-ingestor-route .create-ingestor-relay [data-value='visual_http_relay']" exists
    When selector ".create-ingestor-route .create-ingestor-relay [data-value='visual_http_relay']" is clicked
    And selector ".create-ingestor-route .create-ingestor-inherit [data-inherit='all']" is clicked
    And selector ".create-ingestor-route .create-ingestor-flush [data-flush='immediate']" is clicked
    And selector ".create-ingestor-route .create-ingestor-message-error [data-error='log']" is clicked
    And selector ".create-ingestor-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE INGESTOR visual_http_source FROM HTTP visual_http_client"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_http_subscription TO visual_http_relay;
      START;
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      {"payload":"aligned","user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

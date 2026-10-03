Feature: Web console visual junction and reingestor creation

  @visual_processor_stale_scope
  Scenario: A processor draft requires selected relays again after its domain changes
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_other;
      CREATE SCHEMA visual_scope_event (message STRING);
      CREATE RELAY visual_scope_input SCHEMA visual_scope_event UNBRANCHED;
      CREATE RELAY visual_scope_output SCHEMA visual_scope_event UNBRANCHED;
      """
    And the web console is opened on the leader node
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='junction']" is clicked
    And selector ".create-name" is filled with "visual_scope_junction"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_scope_input"
    Then selector ".create-processor-input [data-value='visual_scope_input']" exists
    When selector ".create-processor-input [data-value='visual_scope_input']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_scope_output"
    Then selector ".create-processor-route-relay [data-value='visual_scope_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_scope_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE ATTACHED JUNCTION visual_scope_junction"
    When selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='junction']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{domain}}_other"
    And selector ".create-name" has value "visual_scope_junction"
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "changed context"
    And selector ".terminal" does not contain "created junction 'visual_scope_junction'"

  @visual_processor_reconnect
  Scenario: A selected junction draft submits after the console follows a new leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_reconnect_event (message STRING);
      CREATE RELAY visual_reconnect_input SCHEMA visual_reconnect_event UNBRANCHED;
      CREATE RELAY visual_reconnect_output SCHEMA visual_reconnect_event UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='junction']" is clicked
    And selector ".create-name" is filled with "visual_reconnected_junction"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_reconnect_input"
    Then selector ".create-processor-input [data-value='visual_reconnect_input']" exists
    When selector ".create-processor-input [data-value='visual_reconnect_input']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_reconnect_output"
    Then selector ".create-processor-route-relay [data-value='visual_reconnect_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_reconnect_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".create-preview" contains "CREATE ATTACHED JUNCTION visual_reconnected_junction"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE JUNCTION visual_reconnected_junction;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE ATTACHED JUNCTION visual_reconnected_junction"

  @visual_processor_staged_order
  Scenario: A junction keeps route order and selects compatible relays staged in its transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_order_event (message STRING);
      CREATE SCHEMA visual_other_event (message STRING, extra STRING);
      CREATE SCHEMA visual_tenant_key (tenant STRING);
      CREATE BRANCH visual_tenant SCHEMA visual_tenant_key TTL 5m;
      CREATE RELAY visual_order_input SCHEMA visual_order_event UNBRANCHED;
      CREATE RELAY visual_order_output SCHEMA visual_order_event UNBRANCHED;
      CREATE RELAY visual_wrong_schema SCHEMA visual_other_event UNBRANCHED;
      CREATE RELAY visual_wrong_branch SCHEMA visual_order_event BRANCHED BY visual_tenant;
      CREATE RELAY visual_order_state SCHEMA visual_order_event UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      """
    And the web console is opened on the leader node
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".prompt-row input" is filled with "CREATE RELAY visual_staged_input SCHEMA visual_order_event UNBRANCHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "CREATE RELAY visual_staged_output SCHEMA visual_order_event UNBRANCHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='junction']" is clicked
    And selector ".create-name" is filled with "visual_order_junction"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_order_input"
    Then selector ".create-processor-input [data-value='visual_order_input']" exists
    When selector ".create-processor-input [data-value='visual_order_input']" is clicked
    And selector ".create-processor-add-input" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_wrong_schema"
    Then selector ".create-processor-input [data-value='visual_wrong_schema']" does not exist
    When selector ".create-processor-input .create-choice-search" is filled with "visual_wrong_branch"
    Then selector ".create-processor-input [data-value='visual_wrong_branch']" does not exist
    When selector ".create-processor-input .create-choice-search" is filled with "visual_staged_input"
    Then selector ".create-processor-input [data-value='visual_staged_input']" exists
    When selector ".create-processor-input [data-value='visual_staged_input']" is clicked
    And selector ".create-processor-add-state" is clicked
    And selector ".create-processor-state-relay .create-choice-search" is filled with "visual_order_state"
    Then selector ".create-processor-state-relay [data-value='visual_order_state']" exists
    When selector ".create-processor-state-relay [data-value='visual_order_state']" is clicked
    And selector ".create-processor-state-policy [data-state-policy='skip']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_order_output"
    Then selector ".create-processor-route-relay [data-value='visual_order_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_order_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-processor-add-route" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_staged_output"
    Then selector ".create-processor-route-relay [data-value='visual_staged_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_staged_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-processor-route-up" is clicked
    Then selector ".create-preview" contains "TO visual_staged_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG TO visual_order_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "3 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "COMPLETE"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector ".prompt-row input" is filled with "SHOW CREATE JUNCTION visual_order_junction;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains
      """
      TO visual_staged_output
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO visual_order_output
      """

  @visual_junction_basic
  Scenario Outline: A visually created junction transforms records from its selected input
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_route_event (message STRING, result STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA visual_route_wire MODE STRICT (message string, result string OPTIONAL);
      CREATE CODEC visual_route_codec FROM WIRE JSON SCHEMA visual_route_wire TO SCHEMA visual_route_event;
      CREATE RELAY visual_route_input SCHEMA visual_route_event UNBRANCHED;
      CREATE RELAY visual_route_output SCHEMA visual_route_event UNBRANCHED;
      CREATE VHOST visual_route_host route-{{test_id}}.example.com;
      CREATE ENDPOINT visual_route_endpoint ON visual_route_host PATH '/route' TYPE HTTP;
      CREATE INGESTOR visual_route_source FROM ENDPOINT visual_route_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_route_codec
        TO visual_route_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='junction']" is clicked
    And selector ".create-name" is filled with "visual_route_junction"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_route_input"
    Then selector ".create-processor-input [data-value='visual_route_input']" exists
    When selector ".create-processor-input [data-value='visual_route_input']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_route_output"
    Then selector ".create-processor-route-relay [data-value='visual_route_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_route_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "result"
    Then selector ".create-processor-output-fields [data-value='result']" exists
    When selector ".create-processor-output-fields [data-value='result']" is clicked
    And selector ".create-processor-assignment-expression" is filled with "input.message"
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE ATTACHED JUNCTION visual_route_junction"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_route_subscription TO visual_route_output;
      START;
      """
    And http payload is posted to host "route-{{test_id}}.example.com" path "/route"
      """
      {"message":"visual junction","result":null}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "message":"visual junction" | "result":"visual junction"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_reingestor_branch
  Scenario Outline: A visually created reingestor repartitions interleaved records
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_partition_event (tenant STRING, message STRING);
      CREATE SCHEMA visual_partition_key (tenant STRING);
      CREATE BRANCH visual_by_tenant SCHEMA visual_partition_key TTL 5m;
      CREATE WIRE JSON SCHEMA visual_partition_wire MODE STRICT (tenant string, message string);
      CREATE CODEC visual_partition_codec FROM WIRE JSON SCHEMA visual_partition_wire TO SCHEMA visual_partition_event;
      CREATE RELAY visual_partition_input SCHEMA visual_partition_event UNBRANCHED;
      CREATE RELAY visual_partition_output SCHEMA visual_partition_event BRANCHED BY visual_by_tenant;
      CREATE VHOST visual_partition_host partition-{{test_id}}.example.com;
      CREATE ENDPOINT visual_partition_endpoint ON visual_partition_host PATH '/partition' TYPE HTTP;
      CREATE INGESTOR visual_partition_source FROM ENDPOINT visual_partition_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_partition_codec
        TO visual_partition_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='reingestor']" is clicked
    And selector ".create-name" is filled with "visual_partition_stage"
    And selector ".create-processor-input .create-choice-search" is filled with "visual_partition_input"
    Then selector ".create-processor-input [data-value='visual_partition_input']" exists
    When selector ".create-processor-input [data-value='visual_partition_input']" is clicked
    And selector ".create-processor-route-branching [data-branch='named']" is clicked
    And selector ".create-processor-route-branch .create-choice-search" is filled with "visual_by_tenant"
    Then selector ".create-processor-route-branch [data-value='visual_by_tenant']" exists
    When selector ".create-processor-route-branch [data-value='visual_by_tenant']" is clicked
    And selector ".create-processor-branch-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-processor-branch-fields [data-value='tenant']" exists
    When selector ".create-processor-branch-fields [data-value='tenant']" is clicked
    And selector ".create-processor-branch-assignment-expression" is filled with "input.tenant"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_partition_output"
    Then selector ".create-processor-route-relay [data-value='visual_partition_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_partition_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE ATTACHED REINGESTOR visual_partition_stage"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_partition_subscription TO visual_partition_output;
      START;
      """
    And http payload is posted to host "partition-{{test_id}}.example.com" path "/partition"
      """
      {"tenant":"a","message":"first"}
      """
    And http payload is posted to host "partition-{{test_id}}.example.com" path "/partition"
      """
      {"tenant":"b","message":"second"}
      """
    And http payload is posted to host "partition-{{test_id}}.example.com" path "/partition"
      """
      {"tenant":"a","message":"third"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"a" | "message":"first"
      "tenant":"b" | "message":"second"
      "tenant":"a" | "message":"third"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_junction_sensitive
  Scenario: A visual junction explicitly leaks one sensitive inherited field
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_sensitive_input (message STRING, secret STRING SENSITIVE);
      CREATE SCHEMA visual_sensitive_output (message STRING, secret STRING);
      CREATE WIRE JSON SCHEMA visual_sensitive_wire MODE STRICT (message string, secret string);
      CREATE CODEC visual_sensitive_codec FROM WIRE JSON SCHEMA visual_sensitive_wire TO SCHEMA visual_sensitive_input;
      CREATE RELAY visual_sensitive_in SCHEMA visual_sensitive_input UNBRANCHED;
      CREATE RELAY visual_sensitive_out SCHEMA visual_sensitive_output UNBRANCHED;
      CREATE VHOST visual_sensitive_host sensitive-{{test_id}}.example.com;
      CREATE ENDPOINT visual_sensitive_endpoint ON visual_sensitive_host PATH '/sensitive' TYPE HTTP;
      CREATE INGESTOR visual_sensitive_source FROM ENDPOINT visual_sensitive_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_sensitive_codec
        TO visual_sensitive_in INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='junction']" is clicked
    And selector ".create-name" is filled with "visual_sensitive_junction"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_sensitive_in"
    Then selector ".create-processor-input [data-value='visual_sensitive_in']" exists
    When selector ".create-processor-input [data-value='visual_sensitive_in']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_sensitive_out"
    Then selector ".create-processor-route-relay [data-value='visual_sensitive_out']" exists
    When selector ".create-processor-route-relay [data-value='visual_sensitive_out']" is clicked
    And selector ".create-processor-inherit [data-inherit='fields']" is clicked
    Then selector ".create-processor-input-fields [data-value='message']" exists
    When selector ".create-processor-input-fields [data-value='message']" is clicked
    And selector ".create-processor-input-fields [data-value='secret']" is clicked
    And selector ".create-processor-inherited-field:has-text('secret') .create-processor-leak" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "INHERIT message, secret LEAK SENSITIVE"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_sensitive_subscription TO visual_sensitive_out;
      START;
      """
    And http payload is posted to host "sensitive-{{test_id}}.example.com" path "/sensitive"
      """
      {"message":"safe","secret":"leaked intentionally"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "message":"safe" | "secret":"leaked intentionally"
      """

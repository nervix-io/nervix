Feature: Web console visual deduplicator and reorderer creation

  @visual_deduplicator_branches
  Scenario Outline: A visually created deduplicator suppresses repeated keys independently in each interleaved branch
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_dedup_event (tenant STRING, event_id STRING, message STRING, note STRING);
      CREATE SCHEMA visual_dedup_key (tenant STRING);
      CREATE BRANCH visual_dedup_tenant SCHEMA visual_dedup_key TTL 5m;
      CREATE WIRE JSON SCHEMA visual_dedup_wire MODE STRICT (tenant string, event_id string, message string, note string);
      CREATE WIRE JSON CODEC visual_dedup_codec FROM WIRE SCHEMA visual_dedup_wire TO SCHEMA visual_dedup_event;
      CREATE RELAY visual_dedup_input SCHEMA visual_dedup_event BRANCHED BY visual_dedup_tenant;
      CREATE RELAY visual_dedup_output SCHEMA visual_dedup_event BRANCHED BY visual_dedup_tenant;
      CREATE VHOST visual_dedup_host dedup-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_dedup_endpoint ON visual_dedup_host PATH '/dedup';
      CREATE HTTP ENDPOINT INGESTOR visual_dedup_ingestor FROM visual_dedup_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_dedup_codec
        TO visual_dedup_input INHERIT ALL BRANCHED BY visual_dedup_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='deduplicator']" is clicked
    And selector ".create-name" is filled with "visual_dedup_stage"
    And selector ".create-processor-branching [data-branch='named']" is clicked
    And selector ".create-processor-branch .create-choice-search" is filled with "visual_dedup_tenant"
    Then selector ".create-processor-branch [data-value='visual_dedup_tenant']" exists
    When selector ".create-processor-branch [data-value='visual_dedup_tenant']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_dedup_input"
    Then selector ".create-processor-input [data-value='visual_dedup_input']" exists
    When selector ".create-processor-input [data-value='visual_dedup_input']" is clicked
    And selector ".create-processor-key-expression" is filled with "input.event_id"
    And selector ".create-processor-max-time" is filled with "10m"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_dedup_output"
    Then selector ".create-processor-route-relay [data-value='visual_dedup_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_dedup_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE DEDUPLICATOR visual_dedup_stage"
    And selector ".create-preview" contains "DEDUPLICATE ON input.event_id MAX TIME 10m BRANCHED BY visual_dedup_tenant"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_dedup_subscription TO visual_dedup_output;
      START;
      """
    And http payload is posted to host "dedup-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"a","event_id":"e1","message":"a-first","note":"note-a1"}
      """
    And http payload is posted to host "dedup-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"b","event_id":"e1","message":"b-first","note":"note-b1"}
      """
    And http payload is posted to host "dedup-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"a","event_id":"e1","message":"a-repeat","note":"note-a-repeat"}
      """
    And http payload is posted to host "dedup-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"b","event_id":"e1","message":"b-repeat","note":"note-b-repeat"}
      """
    And http payload is posted to host "dedup-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"a","event_id":"e2","message":"a-last","note":"note-a2"}
      """
    And http payload is posted to host "dedup-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"b","event_id":"e2","message":"b-last","note":"note-b2"}
      """
    # Each branch delivers in arrival order, so a repeat that was not suppressed arrives before the
    # last record of its branch and fails the step.
    Then within "15s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"a" | "event_id":"e1" | "message":"a-first" | "note":"note-a1"
      "tenant":"b" | "event_id":"e1" | "message":"b-first" | "note":"note-b1"
      "tenant":"a" | "event_id":"e2" | "message":"a-last" | "note":"note-a2"
      "tenant":"b" | "event_id":"e2" | "message":"b-last" | "note":"note-b2"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_reorderer_branches
  Scenario Outline: A visually created reorderer releases each interleaved branch in its BY order
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_reorder_event (tenant STRING, sequence I64, message STRING, note STRING);
      CREATE JAQ JSON CODEC visual_reorder_codec TO SCHEMA visual_reorder_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA visual_reorder_key (tenant STRING);
      CREATE BRANCH visual_reorder_tenant SCHEMA visual_reorder_key TTL 5m;
      CREATE RELAY visual_reorder_input SCHEMA visual_reorder_event BRANCHED BY visual_reorder_tenant;
      CREATE RELAY visual_reorder_output SCHEMA visual_reorder_event BRANCHED BY visual_reorder_tenant;
      CREATE VHOST visual_reorder_host reorder-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_reorder_endpoint ON visual_reorder_host PATH '/reorder';
      CREATE HTTP ENDPOINT INGESTOR visual_reorder_ingestor FROM visual_reorder_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_reorder_codec
        TO visual_reorder_input INHERIT ALL BRANCHED BY visual_reorder_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='reorderer']" is clicked
    And selector ".create-name" is filled with "visual_reorder_stage"
    And selector ".create-processor-branching [data-branch='named']" is clicked
    And selector ".create-processor-branch .create-choice-search" is filled with "visual_reorder_tenant"
    Then selector ".create-processor-branch [data-value='visual_reorder_tenant']" exists
    When selector ".create-processor-branch [data-value='visual_reorder_tenant']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_reorder_input"
    Then selector ".create-processor-input [data-value='visual_reorder_input']" exists
    When selector ".create-processor-input [data-value='visual_reorder_input']" is clicked
    And selector ".create-processor-key-expression" is filled with "input.sequence"
    And selector ".create-processor-max-time" is filled with "10s"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_reorder_output"
    Then selector ".create-processor-route-relay [data-value='visual_reorder_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_reorder_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE REORDERER visual_reorder_stage"
    And selector ".create-preview" contains "BY input.sequence MAX TIME 10s BRANCHED BY visual_reorder_tenant"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_reorder_subscription TO visual_reorder_output;
      START;
      """
    # One unfolded input batch holds both branches out of order.
    And http payload is posted to host "reorder-{{test_id}}.example.com" path "/reorder"
      """
      [
        {"tenant":"a","sequence":3,"message":"a-third","note":"note-a3"},
        {"tenant":"b","sequence":2,"message":"b-second","note":"note-b2"},
        {"tenant":"a","sequence":1,"message":"a-first","note":"note-a1"},
        {"tenant":"b","sequence":1,"message":"b-first","note":"note-b1"},
        {"tenant":"a","sequence":2,"message":"a-second","note":"note-a2"}
      ]
      """
    Then within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "tenant":"a" | "sequence":1 | "message":"a-first" | "note":"note-a1"
      "tenant":"a" | "sequence":2 | "message":"a-second" | "note":"note-a2"
      "tenant":"a" | "sequence":3 | "message":"a-third" | "note":"note-a3"
      "tenant":"b" | "sequence":1 | "message":"b-first" | "note":"note-b1"
      "tenant":"b" | "sequence":2 | "message":"b-second" | "note":"note-b2"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_deduplicator_validation
  Scenario: A deduplicator draft refuses missing keys, time bounds and route clauses and offers only compatible relays
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_check_event (tenant STRING, event_id STRING, message STRING);
      CREATE SCHEMA visual_check_other (tenant STRING, message STRING);
      CREATE SCHEMA visual_check_key (tenant STRING);
      CREATE BRANCH visual_check_tenant SCHEMA visual_check_key TTL 5m;
      CREATE RELAY visual_check_input SCHEMA visual_check_event BRANCHED BY visual_check_tenant;
      CREATE RELAY visual_check_second SCHEMA visual_check_event BRANCHED BY visual_check_tenant;
      CREATE RELAY visual_check_wrong_schema SCHEMA visual_check_other BRANCHED BY visual_check_tenant;
      CREATE RELAY visual_check_unbranched SCHEMA visual_check_event UNBRANCHED;
      CREATE RELAY visual_check_output SCHEMA visual_check_event BRANCHED BY visual_check_tenant;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='deduplicator']" is clicked
    And selector ".create-name" is filled with "visual_check_dedup"
    And selector ".create-processor-branching [data-branch='named']" is clicked
    And selector ".create-processor-branch .create-choice-search" is filled with "visual_check_tenant"
    Then selector ".create-processor-branch [data-value='visual_check_tenant']" exists
    When selector ".create-processor-branch [data-value='visual_check_tenant']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_check"
    Then selector ".create-processor-input [data-value='visual_check_input']" exists
    And selector ".create-processor-input [data-value='visual_check_unbranched']" does not exist
    When selector ".create-processor-input [data-value='visual_check_input']" is clicked
    And selector ".create-processor-add-input" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_check_"
    Then selector ".create-processor-input [data-value='visual_check_second']" exists
    And selector ".create-processor-input [data-value='visual_check_wrong_schema']" does not exist
    And selector ".create-processor-input [data-value='visual_check_unbranched']" does not exist
    When selector ".create-processor-input [data-value='visual_check_second']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_check"
    Then selector ".create-processor-route-relay [data-value='visual_check_output']" exists
    And selector ".create-processor-route-relay [data-value='visual_check_unbranched']" does not exist
    When selector ".create-processor-route-relay [data-value='visual_check_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "DEDUPLICATE ON expression 1"
    When selector ".create-processor-key-expression" is filled with "input.event_id +"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "DEDUPLICATE ON expression 1"
    When selector ".create-processor-key-expression" is filled with "input.missing_field"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "MAX TIME"
    When selector ".create-processor-max-time" is filled with "10m"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "FLUSH IMMEDIATE or FLUSH EACH"
    When selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "ON MESSAGE ERROR"
    When selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "missing_field"
    When selector ".create-processor-key-expression" is filled with "input.event_id"
    And selector ".create-processor-add-key" is clicked
    And selector ".create-processor-key-expression" is filled with "lower(input.message)"
    And selector ".create-processor-key-up" is clicked
    Then selector ".create-preview" contains "FROM visual_check_input, visual_check_second ATTACHED DEDUPLICATE ON lower(input.message), input.event_id MAX TIME 10m"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE DEDUPLICATOR visual_check_dedup;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains
      """
      DEDUPLICATE ON lower(input.message), input.event_id
        MAX TIME 10m
      """

  @visual_reorderer_validation
  Scenario: A reorderer draft refuses a missing BY expression and reports an invalid MAX TIME inline
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_bound_event (sequence I64, message STRING);
      CREATE RELAY visual_bound_input SCHEMA visual_bound_event UNBRANCHED;
      CREATE RELAY visual_bound_output SCHEMA visual_bound_event UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='reorderer']" is clicked
    And selector ".create-name" is filled with "visual_bound_reorderer"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_bound_input"
    Then selector ".create-processor-input [data-value='visual_bound_input']" exists
    When selector ".create-processor-input [data-value='visual_bound_input']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_bound_output"
    Then selector ".create-processor-route-relay [data-value='visual_bound_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_bound_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "BY expression 1"
    When selector ".create-processor-key-expression" is filled with "input.sequence"
    And selector ".create-processor-max-time" is filled with "soon"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "MAX TIME"
    When selector ".create-processor-max-time" is filled with "5s"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"

  @visual_reorderer_transaction
  Scenario: A reorderer keeps its BY order and selects relays staged in its transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_staged_event (priority I32, sequence I64, message STRING);
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".prompt-row input" is filled with "CREATE RELAY visual_staged_events SCHEMA visual_staged_event UNBRANCHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "CREATE RELAY visual_staged_ordered SCHEMA visual_staged_event UNBRANCHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='reorderer']" is clicked
    And selector ".create-name" is filled with "visual_staged_reorderer"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_staged_events"
    Then selector ".create-processor-input [data-value='visual_staged_events']" exists
    When selector ".create-processor-input [data-value='visual_staged_events']" is clicked
    And selector ".create-processor-key-expression" is filled with "input.sequence"
    And selector ".create-processor-add-key" is clicked
    And selector ".create-processor-key-expression" is filled with "input.priority"
    And selector ".create-processor-key-up" is clicked
    And selector ".create-processor-max-time" is filled with "30s"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_staged_ordered"
    Then selector ".create-processor-route-relay [data-value='visual_staged_ordered']" exists
    When selector ".create-processor-route-relay [data-value='visual_staged_ordered']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "BY input.priority, input.sequence MAX TIME 30s UNBRANCHED"
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
    When selector ".prompt-row input" is filled with "SHOW CREATE REORDERER visual_staged_reorderer;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains
      """
      BY input.priority, input.sequence
        MAX TIME 30s
      """

  @visual_deduplicator_stale_scope
  Scenario: A deduplicator draft requires its selected relays again after its domain changes
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_other;
      CREATE SCHEMA visual_scope_event (event_id STRING, message STRING);
      CREATE RELAY visual_scope_input SCHEMA visual_scope_event UNBRANCHED;
      CREATE RELAY visual_scope_output SCHEMA visual_scope_event UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='deduplicator']" is clicked
    And selector ".create-name" is filled with "visual_scope_dedup"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_scope_input"
    Then selector ".create-processor-input [data-value='visual_scope_input']" exists
    When selector ".create-processor-input [data-value='visual_scope_input']" is clicked
    And selector ".create-processor-key-expression" is filled with "input.event_id"
    And selector ".create-processor-max-time" is filled with "1m"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_scope_output"
    Then selector ".create-processor-route-relay [data-value='visual_scope_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_scope_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE DEDUPLICATOR visual_scope_dedup"
    When selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='deduplicator']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{domain}}_other"
    And selector ".create-name" has value "visual_scope_dedup"
    And selector ".create-processor-key-expression" has value "input.event_id"
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "changed context"
    And selector ".terminal" does not contain "visual_scope_dedup"

  @visual_reorderer_reconnect
  Scenario: A selected reorderer draft submits after the console follows a new leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_reconnect_event (sequence I64, message STRING);
      CREATE RELAY visual_reconnect_input SCHEMA visual_reconnect_event UNBRANCHED;
      CREATE RELAY visual_reconnect_output SCHEMA visual_reconnect_event UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When leadership is transferred from node "{{new_leader}}" to node "{{old_leader}}"
    And the web console is opened on node "{{old_leader}}"
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='reorderer']" is clicked
    And selector ".create-name" is filled with "visual_reconnected_reorderer"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_reconnect_input"
    Then selector ".create-processor-input [data-value='visual_reconnect_input']" exists
    When selector ".create-processor-input [data-value='visual_reconnect_input']" is clicked
    And selector ".create-processor-key-expression" is filled with "input.sequence"
    And selector ".create-processor-max-time" is filled with "5s"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_reconnect_output"
    Then selector ".create-processor-route-relay [data-value='visual_reconnect_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_reconnect_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".create-preview" contains "CREATE REORDERER visual_reconnected_reorderer"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE REORDERER visual_reconnected_reorderer;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE REORDERER visual_reconnected_reorderer"

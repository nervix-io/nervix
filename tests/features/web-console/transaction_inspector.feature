Feature: Web console transaction inspector
  Scenario: Inspecting an attached transaction shows its steps and preserves its commit basis
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "transaction"
    When selector ".prompt-row input" is filled with "CREATE SCHEMA inspected_event ( value I64 );"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SCHEMA inspected_event"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is pressed with "Enter"
    When selector ".prompt-row input" is filled with "CREATE SCHEMA inspected_audit ( audit_id U32 );"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SCHEMA inspected_audit"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    And selector ".transaction-inspector" contains "inspected_event"
    And selector ".transaction-inspector" contains "inspected_audit"
    And selector ".transaction-inspector" contains "COMPLETE"
    And selector ".transaction-inspector" contains "DYNAMIC"
    And selector ".transaction-inspector .inspector-item[data-name='inspected_event']" contains "SCHEMA"
    And selector ".transaction-inspector .inspector-item[data-name='inspected_audit']" contains "SCHEMA"
    When the inspector geometry is remembered
    And selector ".transaction-inspector .inspector-controls button:has-text('Before')" is pressed with "Enter"
    Then the inspector geometry matches the remembered drawing
    When selector ".transaction-inspector .inspector-controls button:has-text('After')" is pressed with "Enter"
    Then the inspector geometry matches the remembered drawing
    When selector ".transaction-inspector .inspector-controls button:has-text('Changes')" is pressed with "Enter"
    Then the inspector geometry matches the remembered drawing
    When selector ".transaction-inspector .inspector-controls input[type='search']" is filled with "inspected_event"
    Then inspector search result "inspected_event" is visible in its viewport
    When selector ".transaction-inspector .inspector-fit" is pressed with "Enter"
    Then the inspector drawing fits its viewport
    When selector ".transaction-inspector .step-select" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-scope" contains "effective scope"
    When selector ".transaction-inspector .operation-select:has-text('Operation 1')" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-scope" contains "contribution"
    When selector ".transaction-inspector .inspector-item[data-name='inspected_event']" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-detail" contains "Created"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is pressed with "Enter"
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "COMMIT;"
    And selector ".transaction-indicator" eventually disappears

  Scenario: A stale browser preview is refused before effects and refresh supplies a usable basis
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "contender" is connected to the leader node
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "CREATE SCHEMA staged_event ( value I64 );"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SCHEMA staged_event"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "CURRENT"
    When client "contender" executes these NSPL commands
      """
      CREATE SCHEMA unrelated_event ( value I64 );
      """
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-error" contains "Preview is stale"
    And selector ".transaction-inspector .inspector-summary" contains "STALE"
    When client "contender" fails to execute these NSPL commands
      """
      SHOW CREATE SCHEMA staged_event;
      """
    Then the last command error contains
      """
      schema 'staged_event' does not exist in domain '{{domain}}'
      """
    When selector ".transaction-inspector .inspector-toolbar button:has-text('Refresh')" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "CURRENT"
    When selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "COMMIT;"
    And selector ".transaction-inspector .inspector-summary" contains "COMMITTED"
    When client "contender" executes these NSPL commands
      """
      SHOW CREATE SCHEMA staged_event;
      """
    Then the last command output contains
      """
      CREATE SCHEMA staged_event (
        value I64
      );
      """

  Scenario: Inspecting another discovered transaction leaves the attached commit basis intact
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "other_owner_session" is connected to the leader node
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "CREATE SCHEMA attached_event ( value I64 );"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SCHEMA attached_event"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "CURRENT"
    When client "other_owner_session" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA other_event ( value I64 );
      """
    Then client "other_owner_session" transaction id is saved as placeholder "other_transaction_id"
    When selector ".transaction-inspector .inspector-lookup button:has-text('Discover transactions')" is pressed with "Enter"
    Then selector ".terminal" contains "{{other_transaction_id}}"
    When selector ".transaction-inspector .inspector-lookup input[type='text']" is filled with "{{other_transaction_id}}"
    And selector ".transaction-inspector .inspector-lookup button:has-text('Inspect ID')" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "{{other_transaction_id}}"
    And selector ".transaction-inspector" contains "other_event"
    And selector ".transaction-indicator" contains "Inspect"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is pressed with "Enter"
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "COMMIT;"
    And selector ".transaction-indicator" eventually disappears
    And client "other_owner_session" transaction state is "OPEN"
    When client "other_owner_session" executes these NSPL commands
      """
      REVERT;
      """
    Then client "other_owner_session" transaction state is "REVERTED"

  Scenario: A DESCRIBE result opens retained inspection without attaching its transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA observed_event ( value I64 );
      """
    Then client "owner" transaction id is saved as placeholder "observed_transaction_id"
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "DESCRIBE TRANSACTION '{{observed_transaction_id}}';"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "{{observed_transaction_id}}"
    And selector ".transaction-inspector" contains "observed_event"
    And selector ".transaction-indicator" does not exist
    And client "owner" transaction state is "OPEN"
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then client "owner" transaction state is "COMMITTED"
    When selector ".transaction-inspector .inspector-toolbar button:has-text('Refresh')" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "COMMITTED"
    And selector ".transaction-inspector .inspector-summary" contains "1 accepted, 1 applied"
    And selector ".transaction-inspector .step-select" contains "APPLIED"
    When selector ".transaction-inspector .inspector-controls button:has-text('Actual')" is pressed with "Enter"
    And selector ".transaction-inspector .step-select" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-scope" contains "Actual"
    When the inspector geometry is remembered
    Given client "graph_changer" is connected to the leader node
    When client "graph_changer" executes these NSPL commands
      """
      CREATE SCHEMA unrelated_live_graph_change (value I64);
      """
    Then the inspector geometry matches the remembered drawing

  Scenario: A running schema change frames the whole domain pause
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event_v1 ( seq I64 );
      CREATE RELAY events SCHEMA event_v1 UNBRANCHED;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "ALTER SCHEMA event_v1 ADD FIELD note STRING OPTIONAL;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "ALTER SCHEMA event_v1"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "DOMAIN_PAUSE"
    And selector ".transaction-inspector .inspector-item[data-name='event_v1']" contains "SCHEMA"
    When selector ".transaction-inspector .inspector-controls button:has-text('Planned')" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-domain-label" contains "Whole domain pause"
    When selector ".transaction-inspector .inspector-fit" is pressed with "Enter"
    Then the inspector drawing fits its viewport

  Scenario: A dropped relay and its disconnected relations remain inspectable
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event_v1 ( seq I64 );
      CREATE RELAY events SCHEMA event_v1 UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "DROP RELAY events;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "DROP RELAY events"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector .inspector-item.dropped[data-name='events']" contains "RELAY"
    And selector ".transaction-inspector .inspector-edge.dropped" exists
    When the inspector geometry is remembered
    And selector ".transaction-inspector .inspector-controls button:has-text('After')" is pressed with "Enter"
    Then the inspector geometry matches the remembered drawing
    When selector ".transaction-inspector .inspector-controls button:has-text('Changes')" is pressed with "Enter"
    And selector ".transaction-inspector .inspector-item.dropped[data-name='events']" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-detail" contains "Before"

  Scenario: Parallel data and materialized-state relations keep separate routes and keyboard detail
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA reading_record (sensor STRING, value I64);
      CREATE RELAY readings SCHEMA reading_record UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY enriched_readings SCHEMA reading_record UNBRANCHED;
      CREATE JUNCTION enrich_readings FROM readings
        UNBRANCHED
        USING MATERIALIZED STATE readings REQUIRED SKIP
        TO enriched_readings
          INHERIT ALL
          FLUSH EACH 250ms MAX BATCH SIZE 512kb
          ON MESSAGE ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "DROP JUNCTION enrich_readings;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "DROP JUNCTION enrich_readings"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector .inspector-edge.dropped[data-relation='Topology(Dataflow)'][data-source='readings'][data-target='enrich_readings']" exists
    And selector ".transaction-inspector .inspector-edge.dropped[data-relation='Topology(MaterializedState)'][data-source='readings'][data-target='enrich_readings']" exists
    And selector ".transaction-inspector .inspector-edge.dropped[data-relation='Topology(ConfigurationDependency)'][data-source='enriched_readings'][data-target='enrich_readings']" exists
    And inspector data and materialized-state routes from "readings" to "enrich_readings" are distinct
    When selector ".transaction-inspector .inspector-relation-list summary" is pressed with "Enter"
    And selector ".transaction-inspector .inspector-relation-select[data-relation='Topology(MaterializedState)'][data-source='readings'][data-target='enrich_readings']" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-detail" contains "MaterializedState"

  Scenario: An open inspector follows the attached transaction across leader reconnection
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "CREATE DOMAIN {{domain}};"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "created domain '{{domain}}'"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "CREATE SCHEMA retained_after_reconnect (value I64);"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "quiesce level: DYNAMIC"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector .inspector-item[data-name='retained_after_reconnect']" contains "SCHEMA"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{new_leader}}'"
    And selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    When selector ".transaction-inspector .inspector-toolbar button:has-text('Refresh')" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "CURRENT"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is pressed with "Enter"
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector ".prompt-row input" is filled with "SHOW CREATE SCHEMA retained_after_reconnect;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SCHEMA retained_after_reconnect"

  Scenario: A branched affected graph names its exact branch group
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA branch_event (value I64);
      CREATE SCHEMA branch_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA branch_key TTL 5m;
      CREATE RELAY branch_events SCHEMA branch_event BRANCHED BY by_tenant;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "DROP RELAY branch_events;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "DROP RELAY branch_events"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector .inspector-branch[data-branch='by_tenant']" exists
    And selector ".transaction-inspector .inspector-branch-label" contains "by_tenant"

  Scenario: A retained inspection shows an applying prefix and final actual outcomes
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to node "node-1"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE RESOURCE prefix_bundle;
      CREATE SCHEMA suffix_event (value I64);
      """
    Then client "owner" transaction id is saved as placeholder "partial_transaction_id"
    When the web console is opened on node "node-1"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "DESCRIBE TRANSACTION '{{partial_transaction_id}}';"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    Given transaction commit on node "node-1" pauses after 2 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 2 statement is reached
    And transaction "{{partial_transaction_id}}" eventually has state "COMMITTING"
    When selector ".transaction-inspector .inspector-toolbar button:has-text('Refresh')" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "COMMITTING"
    And selector ".transaction-inspector .inspector-summary" contains "1 applied, 1 pending"
    When the transaction commit pause on node "node-1" after 2 statement is released
    Then the background NSPL execution succeeds
    When selector ".transaction-inspector .inspector-toolbar button:has-text('Refresh')" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "COMMITTED"
    And selector ".transaction-inspector .inspector-summary" contains "2 accepted, 2 applied"
    And selector ".transaction-inspector .step-select" contains "APPLIED"

  Scenario: An incomplete report cannot supply a commit basis until later admission resolves it
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "CREATE RELAY pending_events SCHEMA pending_schema UNBRANCHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE RELAY pending_events"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "INCOMPLETE"
    And selector ".transaction-inspector .inspector-summary" contains "pending_schema"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is pressed with "Enter"
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "Inspect the attached transaction at its current position before COMMIT"
    And selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "CREATE SCHEMA pending_schema (value I64);"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SCHEMA pending_schema"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "COMPLETE"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is pressed with "Enter"
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears

  Scenario: A shared intake gate is drawn once with both contributing operations
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA gated_event (value I64);
      CREATE RELAY intake SCHEMA gated_event UNBRANCHED;
      CREATE RELAY output_a SCHEMA gated_event UNBRANCHED;
      CREATE RELAY output_b SCHEMA gated_event UNBRANCHED;
      CREATE JUNCTION route_a FROM intake UNBRANCHED TO output_a INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION route_b FROM intake UNBRANCHED TO output_b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" contains "Inspect"
    When selector ".prompt-row input" is filled with "ALTER JUNCTION route_a SET DETACHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "ALTER JUNCTION route_a"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is pressed with "Enter"
    And selector ".prompt-row input" is filled with "ALTER JUNCTION route_b SET DETACHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "ALTER JUNCTION route_b"
    When selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    And selector ".transaction-inspector .inspector-item[data-name='intake'] .inspector-marks" contains "GATE" exactly 1 times
    When selector ".transaction-inspector .inspector-item[data-name='intake']" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-detail" contains "operations 1, 2"

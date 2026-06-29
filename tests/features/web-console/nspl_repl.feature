Feature: Web console NSPL REPL
  Scenario: Web console sends simple NSPL commands and renders responses
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-error" contains "NO ACTIVE DATAFLOW GRAPH"
    When selector ".prompt-row input" is filled with "CREATE DOMAIN {{domain}};"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE DOMAIN {{domain}};"
    And selector ".terminal" contains "created domain '{{domain}}'"
    When selector ".prompt-row input" is filled with "SHOW CLUSTER STATUS;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "SHOW CLUSTER STATUS;"
    And selector ".terminal" contains "{{domain}} status=Stopped"
    And selector ".terminal" is scrolled to bottom

  @repl_prompt_viewport
  Scenario: Web console keeps the REPL command row visible in a short viewport
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    And the browser viewport is resized to 1534 by 704
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".prompt-row" is pinned to viewport bottom

  Scenario: Web console submits the edited input value and renders repeated responses
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "CREATE DOMAIN stale_repl_value;"
    And selector ".prompt-row input" is pressed with "Control+A"
    And selector ".prompt-row input" is pressed with "Backspace"
    Then selector ".prompt-row input" has value ""
    When selector ".prompt-row input" is typed with "CREATE DOMAIN {{domain}};"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE DOMAIN {{domain}};"
    And selector ".terminal" contains "created domain '{{domain}}'"
    And selector ".terminal" does not contain "stale_repl_value"
    And selector ".prompt-row input" has value ""
    When selector ".prompt-row input" is typed with "LIST DOMAINS;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "{{domain}} pace=UNPACED status=STOPPED"
    When selector ".prompt-row input" is typed with "LIST DOMAINS;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "LIST DOMAINS;" exactly 2 times
    And selector ".terminal" contains "{{domain}} pace=UNPACED status=STOPPED" exactly 2 times
    And selector ".terminal" is scrolled to bottom

  Scenario: Web console navigates REPL command history with arrow keys
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "CREATE DOMAIN {{domain}};"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "created domain '{{domain}}'"
    When selector ".prompt-row input" is filled with "SHOW CLUSTER STATUS;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "{{domain}} status=Stopped"
    When selector ".prompt-row input" is filled with "SHOW "
    And selector ".prompt-row input" is pressed with "ArrowUp"
    Then selector ".prompt-row input" has value "SHOW CLUSTER STATUS;"
    When selector ".prompt-row input" is pressed with "ArrowUp"
    Then selector ".prompt-row input" has value "CREATE DOMAIN {{domain}};"
    When selector ".prompt-row input" is pressed with "ArrowDown"
    Then selector ".prompt-row input" has value "SHOW CLUSTER STATUS;"
    When selector ".prompt-row input" is pressed with "ArrowDown"
    Then selector ".prompt-row input" has value "SHOW "

  Scenario: Web console submits semicolon-separated command batches
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "CREATE DOMAIN production; CREATE CLIENT http_main TYPE HTTP CONFIG { 'url' = 'http://example.com/a;b' }; CREATE SCHEMA notification ( user_id I64 )"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE DOMAIN production; CREATE CLIENT http_main TYPE HTTP CONFIG { 'url' = 'http://example.com/a;b' }; CREATE SCHEMA notification ( user_id I64 )"
    And selector ".terminal" contains "created domain 'production'"
    And selector ".terminal" contains "stored model 'http_main'"
    And selector ".terminal" contains "stored model 'notification'"
    When selector ".prompt-row input" is filled with "SHOW CREATE SCHEMA notification"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SCHEMA notification (user_id I64);"

  Scenario: Web console commands opened on a follower are handled by the leader
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the web console is opened on node "{{follower}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{leader}}'"
    When selector ".prompt-row input" is filled with "CREATE DOMAIN {{domain}};"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE DOMAIN {{domain}};"
    And selector ".terminal" contains "created domain '{{domain}}'"

  Scenario: Web console command batches opened on a follower are handled by the leader
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the web console is opened on node "{{follower}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{leader}}'"
    When selector ".prompt-row input" is filled with "CREATE DOMAIN production; CREATE SCHEMA follower_notification ( user_id I64 )"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "stored model 'follower_notification'"
    When selector ".prompt-row input" is filled with "SHOW CREATE SCHEMA follower_notification"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SCHEMA follower_notification (user_id I64);"

  Scenario: Web console autocompletes NSPL commands
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "SH"
    Then selector ".suggestions" contains "SHOW"
    When selector ".prompt-row input" is pressed with "Tab"
    Then selector ".prompt-row input" has value "SHOW"
    When selector ".prompt-row input" is filled with "SHOW "
    Then selector ".suggestions" contains "CLUSTER"
    When selector ".prompt-row input" is pressed with "Tab"
    Then selector ".prompt-row input" has value "SHOW CLUSTER"
    When selector ".prompt-row input" is pressed with "Tab"
    Then selector ".prompt-row input" has value "SHOW CREATE"

  Scenario: Web console renders live domain list in the domain selector
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE PACED DOMAIN {{domain}}_paced WITH PERIOD 30s SKEW 1s;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".domain-select" contains "{{domain}}"
    When selector ".domain-select" is clicked
    Then selector ".domain-menu" contains "{{domain}}"
    And selector ".domain-menu" contains "{{domain}}_paced"
    When selector ".prompt-row input" is filled with "LIST DOMAINS;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "LIST DOMAINS;"
    And selector ".terminal" contains "{{domain}} pace=UNPACED status=STOPPED"
    And selector ".terminal" contains "{{domain}}_paced pace=PACED status=STOPPED"

  Scenario: Web console starts and stops the active domain with the lifecycle button
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".summary-row" contains "STOPPED"
    When selector ".topbar-domain-state-button" is clicked
    Then selector ".terminal" contains "START;"
    And selector ".terminal" contains "starting domain '{{domain}}'"
    And selector ".summary-row" contains "RUNNING"
    When selector ".topbar-domain-state-button" is clicked
    Then selector ".terminal" contains "STOP;"
    And selector ".terminal" contains "stopped domain '{{domain}}'"
    And selector ".summary-row" contains "STOPPED"

  Scenario: Web console opens resource upload dialog and uploads a version
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "console_upload_dir" containing
      """
      {
        "alpha.txt": "alpha",
        "nested/beta.txt": "beta"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE console_bundle;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the web console is opened on node "{{follower}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{leader}}'"
    And selector ".graph-error" contains "NO ACTIVE DATAFLOW GRAPH"
    And selector ".nav-item.resources" contains "console_bundle"
    When selector ".nav-item.resources:has-text('console_bundle')" is clicked
    Then selector ".resource-dialog" contains "console_bundle"
    And selector ".resource-dialog" contains "VERSIONS"
    When selector ".resource-dialog .file-upload-input" uploads resource directory "console_upload_dir"
    Then selector ".resource-upload-status" contains "uploaded resource version 1"
    And selector ".resource-version-list" contains "version 1"
    And selector ".resource-version-list" contains "2 files"
    And selector ".resource-version-list" contains "nested"
    And selector ".resource-version-list" contains "nested/beta.txt"
    When selector ".modal-scrim" is clicked by script
    Then selector ".resource-dialog" does not exist

  Scenario: Web console switches domains through the domain selector after REPL domain creation
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "CREATE UNPACED DOMAIN {{domain}};"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "created domain '{{domain}}'"
    And selector ".domain-select" contains "{{domain}}"
    When selector ".prompt-row input" is filled with "CREATE PACED DOMAIN {{domain}}_paced WITH PERIOD 30s SKEW 1s;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "created domain '{{domain}}_paced'"
    When selector ".domain-select" is clicked
    Then selector ".domain-menu" contains "{{domain}}_paced"
    When selector ".domain-menu [data-domain='{{domain}}_paced']" is clicked
    Then selector ".prompt-row" contains "{{domain}}_paced"
    And selector ".domain-select" contains "{{domain}}_paced"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}']" is clicked
    Then selector ".prompt-row" contains "{{domain}}"
    And selector ".domain-select" contains "{{domain}}"

  Scenario: Web console switches between domains without dataflow graphs
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_empty;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-error" contains "NO ACTIVE DATAFLOW GRAPH"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_empty']" is clicked
    Then selector ".prompt-row" contains "{{domain}}_empty"
    And selector ".domain-select" contains "{{domain}}_empty" for 1500 milliseconds
    And selector ".graph-error" contains "NO ACTIVE DATAFLOW GRAPH" for 1500 milliseconds
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}']" is clicked
    Then selector ".prompt-row" contains "{{domain}}"
    And selector ".domain-select" contains "{{domain}}" for 1500 milliseconds
    And selector ".graph-error" contains "NO ACTIVE DATAFLOW GRAPH" for 1500 milliseconds

  Scenario: Web console keeps an empty selected domain when another domain has a graph
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE UNPACED DOMAIN {{domain}}_empty;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}']" is clicked
    Then selector ".prompt-row" contains "{{domain}}"
    And selector ".graph-hit-layer" contains "http_notifications"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_empty']" is clicked
    Then selector ".prompt-row" contains "{{domain}}_empty"
    And selector ".domain-select" contains "{{domain}}_empty" for 2500 milliseconds
    And selector ".graph-hit-layer" does not contain "http_notifications"
    And selector ".graph-error" contains "NO ACTIVE DATAFLOW GRAPH"
    And selector ".graph-error" contains "NO ACTIVE DATAFLOW GRAPH" for 2500 milliseconds
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}']" is clicked
    Then selector ".prompt-row" contains "{{domain}}"
    And selector ".graph-hit-layer" contains "http_notifications"

  Scenario: Web console initially selects a domain that has a graph
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}}_empty;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".prompt-row" contains "{{domain}}"
    And selector ".graph-hit-layer" contains "http_notifications"
    And selector ".graph-error" does not contain "NO ACTIVE DATAFLOW GRAPH"

  Scenario: Web console renders the active dataflow graph
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".prompt-row" contains "{{domain}}"
    And selector ".graph-hit-layer" contains "http_notifications"
    And selector ".graph-hit-layer" contains "notifications"
    And selector ".graph-branch-label-layer" does not contain "tenant_branch"
    And selector ".graph-hit-layer" contains "ENDPOINT"
    And selector ".graph-hit-layer" contains "http_notifications_endpoint"
    And selector ".metrics-strip" does not exist
    And selector ".summary-metrics" contains "0B"

  Scenario: Web console renders a parameterized correlator graph without overlaying the REPL
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA left_profile (
        tenant STRING,
        first_name STRING,
        marker I64
      );
      CREATE SCHEMA right_profile (
        tenant STRING,
        first_name STRING,
        surname STRING
      );
      CREATE SCHEMA correlated_profile (
        tenant STRING,
        normalized_name STRING,
        left_marker I64,
        surname STRING
      );
      CREATE SCHEMA correlator_error (
        error_message STRING,
        failed_node STRING,
        failed_record STRING
      );
      CREATE STRICT WIRE JSON SCHEMA left_profile_wire (
        tenant string,
        first_name string,
        marker integer
      );
      CREATE STRICT WIRE JSON SCHEMA right_profile_wire (
        tenant string,
        first_name string,
        surname string
      );
      CREATE CODEC left_profile_codec FROM WIRE JSON SCHEMA left_profile_wire TO SCHEMA left_profile;
      CREATE CODEC right_profile_codec FROM WIRE JSON SCHEMA right_profile_wire TO SCHEMA right_profile;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY left_profiles SCHEMA left_profile PARAMETERIZED BY tenant_branch;
      CREATE RELAY right_profiles SCHEMA right_profile PARAMETERIZED BY tenant_branch;
      CREATE RELAY correlated_profiles SCHEMA correlated_profile PARAMETERIZED BY tenant_branch;
      CREATE RELAY uncorrelated_left_profiles SCHEMA left_profile PARAMETERIZED BY tenant_branch;
      CREATE RELAY uncorrelated_right_profiles SCHEMA right_profile PARAMETERIZED BY tenant_branch;
      CREATE RELAY correlator_errors SCHEMA correlator_error PARAMETERIZED BY tenant_branch;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT left_ingress ON edge PATH '/left' TYPE HTTP;
      CREATE ENDPOINT right_ingress ON edge PATH '/right' TYPE HTTP;
      CREATE INGESTOR left_profile_ingestor TO left_profiles DECODE USING left_profile_codec PARAMETERIZED BY tenant_branch VALUES { tenant = left_profiles.tenant } TTL 5m FLUSH IMMEDIATE FROM ENDPOINT left_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR right_profile_ingestor TO right_profiles DECODE USING right_profile_codec PARAMETERIZED BY tenant_branch VALUES { tenant = right_profiles.tenant } TTL 5m FLUSH IMMEDIATE FROM ENDPOINT right_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CORRELATOR correlate_profiles
        FROM left_profiles, right_profiles
        CORRELATE WHERE lower(left_profiles.first_name) = lower(right_profiles.first_name)
        MATCH EARLIEST
        TO correlated_profiles PARAMETERIZED BY tenant_branch
        FLUSH IMMEDIATE
        OUTPUT
          correlated_profiles.tenant = left_profiles.tenant,
          correlated_profiles.normalized_name = lower(left_profiles.first_name),
          correlated_profiles.left_marker = left_profiles.marker,
          correlated_profiles.surname = upper(right_profiles.surname)
        MAX TIME 5s
        ON CORRELATION TIMEOUT SEND TO uncorrelated_left_profiles, SEND TO uncorrelated_right_profiles
        ON MESSAGE ERROR DLQ correlator_errors SET error_message = message_error.message, failed_node = message_error.node, failed_record = message_error.record;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "left_profile_ingestor"
    And selector ".graph-hit-layer" contains "right_profile_ingestor"
    And selector ".graph-hit-layer" contains "correlate_profiles"
    And selector ".graph-hit-layer" contains "correlated_profiles"
    And branch group "tenant_branch" has 2 initiator callout and 0 finalizer callout
    And graph edge from "left_profiles" to "correlate_profiles" is visible
    And graph edge from "right_profiles" to "correlate_profiles" is visible
    And graph edge from "correlate_profiles" to "correlated_profiles" is visible
    And graph action edge "correlation timeout" from "correlate_profiles" to "uncorrelated_left_profiles" is visible
    And graph action edge "correlation timeout" from "correlate_profiles" to "uncorrelated_right_profiles" is visible
    And graph action edge "message error" from "correlate_profiles" to "correlator_errors" is visible
    And selector ".graph-branch-header" does not overlap selector ".relay-hit[data-label='left_profiles']"
    And selector ".graph-branch-header" does not overlap selector ".relay-hit[data-label='correlated_profiles']"
    When the browser viewport is resized to 1290 by 560
    Then selector ".prompt-row" is pinned to viewport bottom
    And selector ".graph-panel" does not overlap selector ".prompt-row"
    When selector ".node-hit[data-label='correlate_profiles']" is clicked
    Then selector ".graph-action-menu" contains "CORRELATOR"
    And selector ".graph-action-menu" contains "DESCRIBE"
    When selector ".graph-action-menu button:has-text('DESCRIBE')" is clicked
    Then selector ".terminal" contains "DESCRIBE CORRELATOR correlate_profiles;"
    And selector ".terminal" contains "correlator: correlate_profiles"

  Scenario: Web console graph item actions execute typed NSPL commands
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "http_notifications"
    When selector ".node-hit.ingestor[data-label='http_notifications']" is clicked
    Then selector ".graph-action-menu" contains "INGESTOR"
    And selector ".graph-action-menu" contains "DESCRIBE"
    And selector ".graph-action-menu" contains "SHOW CREATE"
    When selector ".graph-action-menu button:has-text('DESCRIBE')" is clicked
    Then selector ".terminal" contains "DESCRIBE INGESTOR http_notifications;"
    And selector ".terminal" contains "ingestor: http_notifications"
    When selector ".node-hit.ingestor[data-label='http_notifications']" is clicked
    And selector ".graph-action-menu button:has-text('SHOW CREATE')" is clicked
    Then selector ".terminal" contains "SHOW CREATE INGESTOR http_notifications;"
    And selector ".terminal" contains "CREATE INGESTOR http_notifications"
    When selector ".relay-hit:has-text('notifications')" is clicked by script
    Then selector ".graph-action-menu" contains "RELAY"
    And selector ".graph-action-menu" contains "DESCRIBE"
    And selector ".graph-action-menu" contains "SUBSCRIBE"
    When selector ".graph-action-menu button:has-text('DESCRIBE')" is clicked
    Then selector ".terminal" contains "DESCRIBE RELAY notifications;"
    And selector ".terminal" does not contain "expected WHERE"
    When selector ".relay-hit:has-text('notifications')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "notifications"

  Scenario: Web console graph relay subscribe streams in a REPL peer tab
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "notifications"
    When selector ".relay-hit:has-text('notifications')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "notifications"
    And selector ".subscribe-dialog" contains "user_id"
    And selector ".subscribe-dialog" contains "I64"
    And selector ".subscribe-dialog" does not contain "created_at"
    When selector ".schema-field-button:has-text('user_id')" is clicked
    Then selector ".subscribe-dialog input" has value "notifications.user_id"
    When selector ".subscribe-dialog input" is filled with ""
    When selector ".subscribe-actions button:has-text('SUBSCRIBE')" is clicked
    Then selector ".repl-toolbar" contains "NOTIFICATIONS"
    And selector ".terminal" does not contain "using domain"
    And selector ".terminal" does not contain "connected to leader"
    And selector ".terminal" does not contain "SUBSCRIBE notifications;"
    And selector ".terminal" does not contain "parse error"
    When selector ".subscription-tab:has-text('NOTIFICATIONS') .tab-close" is clicked
    Then selector ".repl-toolbar" does not contain "NOTIFICATIONS"
    When selector ".relay-hit:has-text('notifications')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "notifications"
    When selector ".subscribe-dialog input" is filled with "WHERE true"
    And selector ".subscribe-actions button:has-text('SUBSCRIBE')" is clicked
    Then selector ".terminal" does not contain "parse error"
    And selector ".terminal" does not contain "expected SET, UNSET, or WHERE clause"
    When selector ".subscription-tab:has-text('NOTIFICATIONS') .tab-close" is clicked
    Then selector ".repl-toolbar" does not contain "NOTIFICATIONS"

  Scenario Outline: Web console keeps multiple filtered relay subscription tabs isolated
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE SCHEMA notification ( tenant STRING, user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( tenant string, user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "notifications"
    When selector ".relay-hit:has-text('notifications')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "notifications"
    When selector ".subscribe-dialog input" is filled with "WHERE notifications.tenant = 'acme'"
    And selector ".subscribe-actions button:has-text('SUBSCRIBE')" is clicked
    Then selector ".repl-toolbar" contains "NOTIFICATIONS"
    When selector ".relay-hit:has-text('notifications')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "notifications"
    When selector ".subscribe-dialog input" is filled with "WHERE notifications.tenant = 'beta'"
    And selector ".subscribe-actions button:has-text('SUBSCRIBE')" is clicked
    Then selector ".repl-toolbar" contains "ACME"
    And selector ".repl-toolbar" contains "BETA"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":42}
      """
    Then selector ".terminal" does not contain "acme"
    And selector ".terminal" does not contain "[events]"
    And selector ".terminal" does not contain "subscription ["
    And selector ".terminal" does not contain "from ["
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","user_id":7}
      """
    Then selector ".terminal" contains 'key={"tenant":"beta"} payload={'
    And selector ".terminal" contains "beta"
    And selector ".terminal" contains "user_id"
    And selector ".terminal" contains "7"
    And selector ".terminal" does not contain "acme"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":99}
      """
    Then selector ".terminal" does not contain "acme"
    And selector ".terminal" does not contain "99"
    And selector ".terminal" does not contain "[events]"
    And selector ".terminal" does not contain "subscription ["
    And selector ".terminal" does not contain "from ["

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Web console relay subscription tab shows each HTTP-ingested payload once
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNPARAMETERIZED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT raw_metrics_endpoint ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR raw_metrics_source TO raw_metrics DECODE USING metric_codec UNPARAMETERIZED FLUSH IMMEDIATE FROM ENDPOINT raw_metrics_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "raw_metrics"
    When selector ".relay-hit:has-text('raw_metrics')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "raw_metrics"
    When selector ".subscribe-actions button:has-text('SUBSCRIBE')" is clicked
    Then selector ".repl-toolbar" contains "RAW_METRICS"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":3}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":4}
      """
    Then selector ".terminal" contains ":1}"
    And selector ".terminal" contains ":2}"
    And selector ".terminal" contains ":3}"
    And selector ".terminal" contains ":4}"
    And selector ".terminal" contains ":1}" exactly 1 times
    And selector ".terminal" contains ":2}" exactly 1 times
    And selector ".terminal" contains ":3}" exactly 1 times
    And selector ".terminal" contains ":4}" exactly 1 times

  Scenario: Web console keeps relay subscription histories isolated while switching tabs
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA metric (
        value I32,
        rust_keep BOOL,
        go_keep BOOL
      );
      CREATE STRICT WIRE JSON SCHEMA metric_wire (
        value integer,
        rust_keep boolean,
        go_keep boolean
      );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNPARAMETERIZED;
      CREATE RELAY rust_filtered_metrics SCHEMA metric UNPARAMETERIZED;
      CREATE RELAY go_filtered_metrics SCHEMA metric UNPARAMETERIZED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT raw_metrics_endpoint ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR raw_metrics_source TO raw_metrics DECODE USING metric_codec UNPARAMETERIZED FLUSH IMMEDIATE FROM ENDPOINT raw_metrics_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR rust_filter FROM raw_metrics FILTER WHERE raw_metrics.rust_keep TO rust_filtered_metrics UNPARAMETERIZED DEDUPLICATE ON raw_metrics.value MAX TIME 10m FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR go_filter FROM rust_filtered_metrics FILTER WHERE rust_filtered_metrics.go_keep TO go_filtered_metrics UNPARAMETERIZED DEDUPLICATE ON rust_filtered_metrics.value MAX TIME 10m FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "raw_metrics"
    And selector ".graph-hit-layer" contains "rust_filtered_metrics"
    And selector ".graph-hit-layer" contains "go_filtered_metrics"
    When selector ".relay-hit:has-text('raw_metrics')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "raw_metrics"
    When selector ".subscribe-actions button:has-text('SUBSCRIBE')" is clicked
    Then selector ".repl-toolbar" contains "RAW_METRICS"
    When selector ".relay-hit:has-text('rust_filtered_metrics')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "rust_filtered_metrics"
    When selector ".subscribe-actions button:has-text('SUBSCRIBE')" is clicked
    Then selector ".repl-toolbar" contains "RUST_FILTERED_METRICS"
    When selector ".relay-hit:has-text('go_filtered_metrics')" is clicked by script
    And selector ".graph-action-menu button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "go_filtered_metrics"
    When selector ".subscribe-actions button:has-text('SUBSCRIBE')" is clicked
    Then selector ".repl-toolbar" contains "GO_FILTERED_METRICS"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"rust_keep":false,"go_keep":false}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2,"rust_keep":true,"go_keep":false}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":3,"rust_keep":false,"go_keep":false}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":4,"rust_keep":true,"go_keep":true}
      """
    Then selector ".terminal" contains 'value":4'
    And selector ".terminal" contains 'value":4' exactly 1 times
    And selector ".terminal" does not contain 'value":1'
    And selector ".terminal" does not contain 'value":2'
    And selector ".terminal" does not contain 'value":3'
    When selector ".subscription-tab .tab-main[data-subscription-title='rust_filtered_metrics']" is clicked by script
    Then selector ".terminal" contains 'value":2'
    And selector ".terminal" contains 'value":4'
    And selector ".terminal" contains 'value":2' exactly 1 times
    And selector ".terminal" contains 'value":4' exactly 1 times
    And selector ".terminal" does not contain 'value":1'
    And selector ".terminal" does not contain 'value":3'
    When selector ".subscription-tab .tab-main[data-subscription-title='raw_metrics']" is clicked by script
    Then selector ".terminal" contains 'value":1'
    And selector ".terminal" contains 'value":2'
    And selector ".terminal" contains 'value":3'
    And selector ".terminal" contains 'value":4'
    And selector ".terminal" contains 'value":1' exactly 1 times
    And selector ".terminal" contains 'value":2' exactly 1 times
    And selector ".terminal" contains 'value":3' exactly 1 times
    And selector ".terminal" contains 'value":4' exactly 1 times
    When selector ".subscription-tab .tab-main[data-subscription-title='go_filtered_metrics']" is clicked by script
    Then selector ".terminal" contains 'value":4'
    And selector ".terminal" contains 'value":4' exactly 1 times
    And selector ".terminal" does not contain 'value":1'
    And selector ".terminal" does not contain 'value":2'
    And selector ".terminal" does not contain 'value":3'

  Scenario: Web console graph processor actions describe deduplicators and reorderers
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE RELAY notifications SCHEMA notification UNPARAMETERIZED;
      CREATE RELAY priority_notifications SCHEMA notification UNPARAMETERIZED;
      CREATE RELAY default_notifications SCHEMA notification UNPARAMETERIZED;
      CREATE RELAY ordered_notifications SCHEMA notification UNPARAMETERIZED;
      CREATE DEDUPLICATOR route_notifications FROM notifications TO priority_notifications WHERE notifications.user_id = 1 TO default_notifications UNPARAMETERIZED DEDUPLICATE ON notifications.user_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE REORDERER order_notifications FROM default_notifications TO ordered_notifications UNPARAMETERIZED BY default_notifications.user_id MAX TIME 10s FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "route_notifications"
    And selector ".graph-hit-layer" contains "order_notifications"
    When selector ".node-hit:has-text('route_notifications')" is clicked
    Then selector ".graph-action-menu" contains "DEDUPLICATOR"
    And selector ".graph-action-menu" contains "DESCRIBE"
    When selector ".graph-action-menu button:has-text('DESCRIBE')" is clicked
    Then selector ".terminal" contains "DESCRIBE DEDUPLICATOR route_notifications;"
    And selector ".terminal" contains "deduplicator: route_notifications"
    When selector ".node-hit:has-text('order_notifications')" is clicked
    Then selector ".graph-action-menu" contains "REORDERER"
    And selector ".graph-action-menu" contains "DESCRIBE"
    When selector ".graph-action-menu button:has-text('DESCRIBE')" is clicked
    Then selector ".terminal" contains "DESCRIBE REORDERER order_notifications;"
    And selector ".terminal" contains "reorderer: order_notifications"

  Scenario: Web console shows graph client error status
    Given a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT zeromq_main TYPE ZEROMQ CONFIG { 'addr' = '{{zeromq_emit_addr}}', 'bind' = 'false' };
      CREATE EMITTER zeromq_notifications FROM notifications ENCODE USING notification_codec TO ZEROMQ zeromq_main ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      START;
      """
    And the web console is opened on the leader node
    Then graph item "http_notifications" has status "OK"
    And graph item "zeromq_notifications" has status "OK"
    When ingestor "http_notifications" enters fault mode
    Then graph item "http_notifications" has status "ERROR"
    When ingestor "http_notifications" leaves fault mode
    Then graph item "http_notifications" has status "OK"
    When emitter "zeromq_notifications" enters stall mode
    And http payload is posted to host "api.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then graph item "zeromq_notifications" has status "ERROR"
    When emitter "zeromq_notifications" leaves fault mode
    Then graph item "zeromq_notifications" has status "OK"

  Scenario: Web console renders active domain entities beside the graph
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE fraud_model;
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE CLIENT http_main TYPE HTTP CONFIG { 'url' = 'http://example.com/ingest' };
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE RELAY notifications SCHEMA notification;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".nav-list" contains "notification"
    And selector ".nav-list" contains "notification_wire"
    And selector ".nav-list" contains "notification_codec"
    And selector ".nav-list" contains "http_main"
    And selector ".nav-list" contains "edge"
    And selector ".nav-list" contains "http_notifications_endpoint"
    And selector ".nav-list" contains "fraud_model"
    And selector ".nav-list" does not contain "event_raw"
    When selector ".nav-item:has-text('http_notifications_endpoint')" is clicked
    Then selector ".terminal" contains "DESCRIBE ENDPOINT http_notifications_endpoint;"
    And selector ".terminal" contains "endpoint: http_notifications_endpoint"
    And selector ".terminal" contains "path: /ingest"

  Scenario: Web console receives graph snapshots after it is already connected
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-error" contains "NO ACTIVE DATAFLOW GRAPH"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then selector ".graph-hit-layer" contains "http_notifications"
    And selector ".graph-hit-layer" contains "notifications"
    And selector ".graph-hit-layer" contains "ENDPOINT"
    And selector ".graph-hit-layer" contains "http_notifications_endpoint"

  Scenario: Web console lays out datalake-style source edges and long nodes
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA activity (
        tenant_id STRING,
        device_id STRING,
        event_type STRING,
        value STRING
      );
      CREATE STRICT WIRE JSON SCHEMA activity_wire (
        tenant_id string,
        device_id string,
        event_type string,
        value string
      );
      CREATE CODEC activity_codec FROM WIRE JSON SCHEMA activity_wire TO SCHEMA activity;
      CREATE IF NOT EXISTS SCHEMA device_branch ( tenant_id STRING, device_id STRING );
      CREATE RELAY device_activity_landing SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE RELAY edge_activity_landing SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE RELAY auth_activity_landing SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE RELAY edge_activity_enriched_landing SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE RELAY edge_connect_events SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE RELAY edge_disconnect_events SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE RELAY security_events SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE RELAY connected_sessions SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE RELAY location_distance_alerts SCHEMA activity PARAMETERIZED BY device_branch;
      CREATE CLIENT kafka_auth TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT mqtt_devices TYPE MQTT CONFIG { 'addr' = 'mqtt://127.0.0.1:1883' };
      CREATE CLIENT nats_edge TYPE NATS CONFIG { 'addr' = 'nats://127.0.0.1:4222' };
      CREATE CLIENT s3_lakehouse TYPE S3 CONFIG {
        'endpoint' = 'http://127.0.0.1:9900',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE CLIENT lakehouse_catalog TYPE ICEBERG_REST CONFIG {
        'uri' = 'http://127.0.0.1:8181',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE INGESTOR iot_device_activity
        TO device_activity_landing
        DECODE USING activity_codec
        PARAMETERIZED BY device_branch VALUES {
          tenant_id = device_activity_landing.tenant_id,
          device_id = device_activity_landing.device_id
        } TTL 30m
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        FROM MQTT mqtt_devices
        TOPIC 'datalake/device_activity'
        INSTANCES 2
        MODE NO_ACK SEQUENTIAL
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR edge_server_activity
        TO edge_activity_landing
        DECODE USING activity_codec
        PARAMETERIZED BY device_branch VALUES {
          tenant_id = edge_activity_landing.tenant_id,
          device_id = edge_activity_landing.device_id
        } TTL 30m
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        FROM NATS nats_edge
        SUBJECT datalake_edge_activity
        QUEUE GROUP datalake_edge_servers
        INSTANCES 2
        MODE NO_ACK SEQUENTIAL
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR auth_server_activity
        TO auth_activity_landing
        DECODE USING activity_codec
        PARAMETERIZED BY device_branch VALUES {
          tenant_id = auth_activity_landing.tenant_id,
          device_id = auth_activity_landing.device_id
        } TTL 30m
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        FROM KAFKA kafka_auth
        TOPIC datalake_auth_activity
        OFFSET BY CONSUMER GROUP datalake_demo_auth
        INSTANCES 4
        MODE NO_ACK PARALLEL MAX 1024
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR connect_without_authorization
        FROM auth_activity_landing
        TO security_events
        PARAMETERIZED BY device_branch
        DEDUPLICATE ON auth_activity_landing.tenant_id, auth_activity_landing.device_id, auth_activity_landing.event_type, auth_activity_landing.value
        MAX TIME 30m
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR connection_distance_alert_mapper
        FROM device_activity_landing
        TO connected_sessions
        PARAMETERIZED BY device_branch
        DEDUPLICATE ON device_activity_landing.tenant_id, device_activity_landing.device_id, device_activity_landing.event_type, device_activity_landing.value
        MAX TIME 30m
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR location_distance_alert_mapper
        FROM device_activity_landing
        TO location_distance_alerts
        PARAMETERIZED BY device_branch
        DEDUPLICATE ON device_activity_landing.tenant_id, device_activity_landing.device_id, device_activity_landing.event_type, device_activity_landing.value
        MAX TIME 30m
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR edge_location_lookup
        FROM edge_activity_landing
        TO edge_activity_enriched_landing
        PARAMETERIZED BY device_branch
        DEDUPLICATE ON edge_activity_landing.tenant_id, edge_activity_landing.device_id, edge_activity_landing.event_type, edge_activity_landing.value
        MAX TIME 30m
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR edge_activity_splitter
        FROM edge_activity_enriched_landing
        TO edge_connect_events WHERE edge_activity_enriched_landing.event_type = "connect"
        TO edge_disconnect_events WHERE edge_activity_enriched_landing.event_type = "disconnect"
        TO edge_connect_events
        PARAMETERIZED BY device_branch
        DEDUPLICATE ON edge_activity_enriched_landing.tenant_id, edge_activity_enriched_landing.device_id, edge_activity_enriched_landing.event_type, edge_activity_enriched_landing.value
        MAX TIME 30m
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG;
      CREATE EMITTER kafka_security_events
        FROM security_events
        ENCODE USING activity_codec
        TO KAFKA kafka_auth TOPIC datalake_security_events
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 1s MAX BATCH SIZE 1MiB;
      CREATE DETACHED EMITTER iceberg_connected_sessions
        FROM connected_sessions
        TO ICEBERG ON S3 s3_lakehouse TABLE datalake_connected_sessions
        VALUES {
          'tenant_id' = connected_sessions.tenant_id,
          'device_id' = connected_sessions.device_id,
          'event_type' = connected_sessions.event_type,
          'value' = connected_sessions.value
        }
        LOCATION 's3://nervix-iceberg/tables/datalake_connected_sessions'
        CATALOG lakehouse_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 10s MAX BATCH SIZE 8MiB
        COMMIT EACH 1m MAX SIZE 512MiB;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And graph edge from "kafka_auth" to "auth_server_activity" is visible
    And graph edge from "kafka_auth" to "auth_server_activity" has exact hover target
    And graph edge from "mqtt_devices" to "iot_device_activity" is visible
    And graph edge from "nats_edge" to "edge_server_activity" is visible
    And graph edge from "auth_activity_landing" to "connect_without_authorization" is visible
    And graph edge from "auth_activity_landing" to "connect_without_authorization" has target plug at least 64 pixels
    And graph edge from "connect_without_authorization" to "security_events" is visible
    And graph edge from "kafka_security_events" to "kafka_auth" is visible
    When graph edge from "kafka_security_events" to "kafka_auth" is clicked with viewport focused on its middle
    Then graph edge from "kafka_security_events" to "kafka_auth" has both endpoints visible in the graph viewport
    When selector ".graph-search input" is filled with "d"
    Then graph item "iot_device_activity" is not highlighted by graph search
    When selector ".graph-search input" is filled with "device"
    Then graph item "mqtt_devices" is highlighted by graph search
    And graph item "iot_device_activity" is highlighted by graph search
    And graph item "device_activity_landing" is highlighted by graph search
    And graph search highlights exactly 3 graph items
    And graph search result "mqtt_devices" is visible in the graph viewport
    And graph search result "iot_device_activity" is visible in the graph viewport
    And graph search result "device_activity_landing" is visible in the graph viewport
    When selector ".graph-search-clear" is clicked
    Then selector ".graph-search input" has value ""
    And graph search highlights exactly 0 graph items
    And graph item "mqtt_devices" is not highlighted by graph search
    And graph item "iot_device_activity" is not highlighted by graph search
    And graph item "device_activity_landing" is not highlighted by graph search
    And graph item "connect_without_authorization" has graph width at least 180 pixels
    And graph item "connection_distance_alert_mapper" has graph width at least 180 pixels
    And graph item "iceberg_connected_sessions" has graph width at least 180 pixels
    And graph item "kafka_auth" does not overlap graph item "mqtt_devices"
    And graph item "mqtt_devices" does not overlap graph item "nats_edge"
    And graph edge from "kafka_auth" to "auth_server_activity" starts horizontally
    And graph edge from "mqtt_devices" to "iot_device_activity" starts horizontally
    And graph edge from "nats_edge" to "edge_server_activity" starts horizontally
    And graph edge from "mqtt_devices" to "iot_device_activity" does not intersect graph edge from "nats_edge" to "edge_server_activity"
    And graph edge from "edge_activity_enriched_landing" to "edge_activity_splitter" uses a direct curve
    And graph edge from "device_activity_landing" to "connection_distance_alert_mapper" has source plug at least 40 pixels
    And graph edge from "device_activity_landing" to "connection_distance_alert_mapper" has target plug at least 60 pixels
    And graph edge from "device_activity_landing" to "connection_distance_alert_mapper" has at most 2 rounded turns
    And graph edge from "device_activity_landing" to "location_distance_alert_mapper" has source plug at least 40 pixels
    And graph edge from "device_activity_landing" to "location_distance_alert_mapper" has target plug at least 60 pixels
    And graph edge from "device_activity_landing" to "location_distance_alert_mapper" has at most 2 rounded turns
    And graph edge from "device_activity_landing" to "connection_distance_alert_mapper" does not share horizontal lane with graph edge from "device_activity_landing" to "location_distance_alert_mapper"

  Scenario: Web console keeps branch callouts aligned after adding an ingestor live
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "leader"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA telemetry ( site STRING, value STRING );
      CREATE STRICT WIRE JSON SCHEMA telemetry_wire ( site string, value string );
      CREATE CODEC telemetry_codec FROM WIRE JSON SCHEMA telemetry_wire TO SCHEMA telemetry;
      CREATE RELAY telemetry_by_site SCHEMA telemetry;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT primary_telemetry_endpoint ON edge PATH '/primary' TYPE HTTP;
      CREATE IF NOT EXISTS SCHEMA site_branch ( site STRING );
      CREATE INGESTOR primary_telemetry TO telemetry_by_site DECODE USING telemetry_codec PARAMETERIZED BY site_branch VALUES { site = telemetry_by_site.site } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT primary_telemetry_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "primary_telemetry"
    And selector ".graph-branch-header" contains "site_branch"
    And selector ".graph-branch-header" contains "0 br"
    And selector ".graph-branch-header" contains "keys site"
    When these NSPL commands are executed on the leader node
      """
      CREATE ENDPOINT backup_telemetry_endpoint ON edge PATH '/backup' TYPE HTTP;
      CREATE INGESTOR backup_telemetry TO telemetry_by_site DECODE USING telemetry_codec PARAMETERIZED BY site_branch VALUES { site = telemetry_by_site.site } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT backup_telemetry_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then selector ".graph-hit-layer" contains "backup_telemetry"
    And branch group "site_branch" has 2 initiator callout and 0 finalizer callout
    And branch group "site_branch" left callout points to graph item "backup_telemetry"
    Then node "{{leader}}" eventually accepts http traffic for host "api.example.com" path "/primary"
      """
      { "site": "iad-1", "value": "71" }
      """
    And node "{{leader}}" eventually accepts http traffic for host "api.example.com" path "/backup"
      """
      { "site": "sfo-1", "value": "68" }
      """
    Then selector ".graph-branch-header" contains "2 br"
    When selector ".graph-branch-header" is clicked
    Then selector ".branch-dialog" contains "site_branch"
    And selector ".branch-dialog" contains "active branches"
    And selector ".branch-dialog" contains "2"

  Scenario: Web console renders a deduplicator between two relays
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value STRING );
      CREATE STRICT WIRE JSON SCHEMA event_wire ( value string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE IF NOT EXISTS SCHEMA value_branch ( value STRING );
      CREATE RELAY raw_events SCHEMA event PARAMETERIZED BY value_branch;
      CREATE RELAY deduped_events SCHEMA event PARAMETERIZED BY value_branch;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT raw_events_endpoint ON edge PATH '/raw' TYPE HTTP;
      CREATE IF NOT EXISTS SCHEMA value_branch ( value STRING ); CREATE INGESTOR ingest_events TO raw_events DECODE USING event_codec PARAMETERIZED BY value_branch VALUES { value = raw_events.value } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT raw_events_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_events FROM raw_events TO deduped_events PARAMETERIZED BY value_branch DEDUPLICATE ON raw_events.value MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "raw_events"
    And selector ".relay-hit" contains "raw_events"
    And selector ".node-hit[data-label='raw_events']" does not exist
    And selector ".graph-hit-layer" contains "dedup_events"
    And selector ".graph-hit-layer" contains "deduped_events"
    And selector ".relay-hit" contains "deduped_events"
    And selector ".node-hit[data-label='deduped_events']" does not exist
    And selector ".graph-hit-layer" contains "raw_events_endpoint"

  Scenario: Web console renders a WASM processor between two relays
    Given a 1 node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      CREATE SCHEMA event ( value STRING );
      CREATE STRICT WIRE JSON SCHEMA event_wire ( value string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY raw_events SCHEMA event UNPARAMETERIZED;
      CREATE RELAY filtered_events SCHEMA event UNPARAMETERIZED;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT raw_events_endpoint ON edge PATH '/raw' TYPE HTTP;
      CREATE INGESTOR ingest_events TO raw_events DECODE USING event_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT raw_events_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_events USING RESOURCE wasm_filter VERSION 1 FILE 'processors/filter_even.wasm' FROM raw_events TO filtered_events UNPARAMETERIZED ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "filter_events"
    And selector ".node-hit" contains "filter_events"
    And selector ".relay-hit" does not contain "filter_events"
    And graph edge from "raw_events" to "filter_events" is visible
    And graph edge from "filter_events" to "filtered_events" is visible
    When selector ".node-hit:has-text('filter_events')" is clicked
    Then selector ".graph-action-menu" contains "WASM PROCESSOR"
    When selector ".graph-action-menu button:has-text('DESCRIBE')" is clicked
    Then selector ".terminal" contains "DESCRIBE WASM PROCESSOR filter_events;"
    And selector ".terminal" contains "wasm processor: filter_events"

  Scenario: Web console renders branch groups across a reingestor
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );
      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        user_id integer
      );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_user_id_branch;
      CREATE RELAY validated_notifications SCHEMA notification PARAMETERIZED BY tenant_user_id_branch;
      CREATE RELAY tenant_notifications SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT reingestor_metrics_ingress ON edge PATH '/reingestor-metrics' TYPE HTTP;
      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
      CREATE INGESTOR reingestor_metrics_source
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT reingestor_metrics_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR notification_forwarder
        FROM notifications
        TO validated_notifications
        PARAMETERIZED BY tenant_user_id_branch
        DEDUPLICATE ON notifications.tenant, notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE REINGESTOR reingestor_metrics_node
        FROM validated_notifications
        TO tenant_notifications
        PARAMETERIZED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "reingestor_metrics_source"
    And selector ".graph-hit-layer" contains "notification_forwarder"
    And selector ".graph-hit-layer" contains "reingestor_metrics_node"
    And selector ".graph-hit-layer" contains "tenant_notifications"
    And selector ".graph-branch-label-layer" contains "tenant_user_id_branch"
    And selector ".graph-branch-label-layer" contains "tenant_branch"
    And branch group "tenant_user_id_branch" has 1 initiator callout and 1 finalizer callout
    And branch group "tenant_user_id_branch" body overlaps graph item "notification_forwarder"
    And branch group "tenant_user_id_branch" left callout points to graph item "reingestor_metrics_source"
    And branch group "tenant_user_id_branch" right callout points to graph item "reingestor_metrics_node"
    And branch group "tenant_user_id_branch" body does not overlap graph item "reingestor_metrics_source"
    And branch group "tenant_user_id_branch" body does not overlap graph item "reingestor_metrics_node"
    And branch group "tenant_branch" has 1 initiator callout and 0 finalizer callout
    And branch group "tenant_branch" body does not overlap graph item "reingestor_metrics_node"

  Scenario: Web console routes shared sink client edges around downstream branch groups
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA telemetry (
        site STRING,
        device_id STRING,
        battery_pct F64,
        value F64,
        warn_high F64
      );
      CREATE STRICT WIRE JSON SCHEMA telemetry_wire (
        site string,
        device_id string,
        battery_pct number,
        value number,
        warn_high number
      );
      CREATE CODEC telemetry_codec FROM WIRE JSON SCHEMA telemetry_wire TO SCHEMA telemetry;
      CREATE IF NOT EXISTS SCHEMA site_branch ( site STRING );
      CREATE IF NOT EXISTS SCHEMA device_branch ( device_id STRING );
      CREATE RELAY telemetry_by_site SCHEMA telemetry PARAMETERIZED BY site_branch;
      CREATE RELAY battery_alerts SCHEMA telemetry PARAMETERIZED BY site_branch;
      CREATE RELAY telemetry_clean SCHEMA telemetry PARAMETERIZED BY site_branch;
      CREATE RELAY telemetry_by_device SCHEMA telemetry PARAMETERIZED BY device_branch;
      CREATE RELAY maintenance_alerts SCHEMA telemetry PARAMETERIZED BY device_branch;
      CREATE RELAY normal_telemetry SCHEMA telemetry PARAMETERIZED BY device_branch;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT telemetry_ingress ON edge PATH '/telemetry' TYPE HTTP;
      CREATE CLIENT redis_alerts TYPE REDIS CONFIG { 'addr' = 'redis://127.0.0.1:6379/' };
      CREATE INGESTOR http_telemetry
        TO telemetry_by_site
        DECODE USING telemetry_codec
        PARAMETERIZED BY site_branch VALUES { site = telemetry_by_site.site } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT telemetry_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR quality_gate
        FROM telemetry_by_site
        TO battery_alerts WHERE telemetry_by_site.battery_pct < 15.0
        TO telemetry_clean
        PARAMETERIZED BY site_branch
        DEDUPLICATE ON telemetry_by_site.site, telemetry_by_site.device_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE REINGESTOR device_repartition
        FROM telemetry_clean
        TO telemetry_by_device
        PARAMETERIZED BY device_branch VALUES { device_id = telemetry_by_device.device_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR anomaly_splitter
        FROM telemetry_by_device
        TO maintenance_alerts WHERE telemetry_by_device.value >= telemetry_by_device.warn_high
        TO normal_telemetry
        PARAMETERIZED BY device_branch
        DEDUPLICATE ON telemetry_by_device.device_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE EMITTER redis_battery_alerts
        FROM battery_alerts
        ENCODE USING telemetry_codec
        TO REDIS PUBSUB redis_alerts CHANNEL battery_alerts
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      CREATE EMITTER redis_maintenance_alerts
        FROM maintenance_alerts
        ENCODE USING telemetry_codec
        TO REDIS PUBSUB redis_alerts CHANNEL maintenance_alerts
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "redis_battery_alerts"
    And selector ".graph-hit-layer" contains "redis_maintenance_alerts"
    And selector ".graph-hit-layer" contains "redis_alerts"
    And branch group "device_branch" has 1 initiator callout and 1 finalizer callout
    And graph edge from "quality_gate" to "battery_alerts" starts horizontally
    And graph edge from "quality_gate" to "battery_alerts" ends horizontally
    And graph edge from "anomaly_splitter" to "maintenance_alerts" starts horizontally
    And graph edge from "anomaly_splitter" to "maintenance_alerts" ends horizontally
    And graph edge from "anomaly_splitter" to "maintenance_alerts" uses a direct curve
    And graph edge from "telemetry_clean" to "device_repartition" does not intersect graph edge from "battery_alerts" to "redis_battery_alerts"
    And graph edge from "redis_battery_alerts" to "redis_alerts" is visible
    And graph edge from "redis_battery_alerts" to "redis_alerts" starts horizontally
    And graph edge from "redis_battery_alerts" to "redis_alerts" ends horizontally
    And graph edge from "redis_battery_alerts" to "redis_alerts" does not intersect branch group "device_branch" body
    And graph edge from "redis_battery_alerts" to "redis_alerts" does not intersect graph item "redis_maintenance_alerts"

  @relay_buffer_statistics
  Scenario: Web console exposes relay buffer distribution
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY user_id_branch CAPACITY 3;
      CREATE RELAY forwarded_notifications SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE VHOST edge http-{{test_id}}-buffer.example.com;
      CREATE ENDPOINT relay_buffer_ingress ON edge PATH '/relay-buffer' TYPE HTTP;
      CREATE INGESTOR relay_buffer_source
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT relay_buffer_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR relay_buffer_forwarder
        FROM notifications
        TO forwarded_notifications
        PARAMETERIZED BY user_id_branch
        DEDUPLICATE ON notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      SUBSCRIBE SESSION TO notifications WHERE notifications.user_id = 42;
      START;
      """
    And http payload is posted to host "http-{{test_id}}-buffer.example.com" path "/relay-buffer"
      """
      {"user_id":42}
      """
    And the web console is opened on the leader node
    Then selector ".graph-hit-layer" contains "relay_buffer_ingress"
    And graph edge from "relay_buffer_ingress" to "relay_buffer_source" is visible
    And graph edge from "relay_buffer_ingress" to "relay_buffer_source" has traffic statistics
      """
      messages_total>=1
      bytes_total>=1
      batches_total=0
      """
    And graph edge from "relay_buffer_source" to "notifications" has traffic statistics
      """
      messages_total>=1
      bytes_total>=1
      batches_total>=1
      """
    When graph topology render count observation starts
    And http payload is posted to host "http-{{test_id}}-buffer.example.com" path "/relay-buffer"
      """
      {"user_id":42}
      """
    Then graph edge from "relay_buffer_source" to "notifications" has traffic statistics
      """
      messages_total>=2
      bytes_total>=1
      batches_total>=2
      """
    And graph topology render count does not change during observed traffic
    And graph relay item "notifications" has buffer statistics
      """
      capacity=3
      p50>=0
      p90>=0
      p99>=0
      """

  Scenario: Web console relayouts existing graph items after incremental snapshots
    Given a 3 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA txn ( value STRING );
      CREATE STRICT WIRE JSON SCHEMA txn_wire ( value string );
      CREATE CODEC txn_codec FROM WIRE JSON SCHEMA txn_wire TO SCHEMA txn;
      CREATE IF NOT EXISTS SCHEMA value_branch ( value STRING );
      CREATE RELAY ss1 SCHEMA txn PARAMETERIZED BY value_branch;
      CREATE RELAY ss2 SCHEMA txn PARAMETERIZED BY value_branch;
      CREATE VHOST edge api.example.com;
      CREATE ENDPOINT source_txns_endpoint ON edge PATH '/source' TYPE HTTP;
      CREATE IF NOT EXISTS SCHEMA value_branch ( value STRING ); CREATE INGESTOR source_txns TO ss1 DECODE USING txn_codec PARAMETERIZED BY value_branch VALUES { value = ss1.value } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT source_txns_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_txns FROM ss1 TO ss2 PARAMETERIZED BY value_branch DEDUPLICATE ON ss1.value MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """
    Then selector ".graph-hit-layer" contains "source_txns"
    And selector ".graph-hit-layer" contains "dedup_txns"
    When these NSPL commands are executed on the leader node
      """
      CREATE RELAY state_txns SCHEMA txn PARAMETERIZED BY value_branch WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY rr1 SCHEMA txn PARAMETERIZED BY value_branch;
      CREATE ENDPOINT state_txns_endpoint ON edge PATH '/state' TYPE HTTP;
      CREATE IF NOT EXISTS SCHEMA value_branch ( value STRING ); CREATE INGESTOR state_txns_ingestor TO state_txns DECODE USING txn_codec PARAMETERIZED BY value_branch VALUES { value = state_txns.value } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT state_txns_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR fwd FROM ss2 TO rr1 PARAMETERIZED BY value_branch DEDUPLICATE ON ss2.value MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """
    Then selector ".graph-hit-layer" contains "state_txns_ingestor"
    And selector ".graph-hit-layer" contains "state_txns"
    And selector ".graph-hit-layer" contains "fwd"
    And selector ".graph-hit-layer" contains "ss1"
    And selector ".relay-hit" contains "ss1"
    And selector ".node-hit" does not contain "ss1"
    And selector ".graph-hit-layer" contains "ss2"
    And selector ".relay-hit" contains "ss2"
    And selector ".node-hit" does not contain "ss2"
    And selector ".graph-hit-layer" contains "rr1"
    And selector ".relay-hit" contains "rr1"
    And selector ".node-hit" does not contain "rr1"
    And selector ".graph-hit-layer" does not contain "materializer"
    And selector ".graph-hit-layer" contains "source_txns_endpoint"
    And selector ".graph-hit-layer" contains "state_txns_endpoint"
    And selector ".graph-hit-layer" contains "ENDPOINT"
    And graph item "source_txns" does not overlap graph item "state_txns_ingestor"
    And graph item "ss1" does not overlap graph item "state_txns"
    And graph item "fwd" does not overlap graph item "rr1"
    And graph edge from "fwd" to "rr1" is visible

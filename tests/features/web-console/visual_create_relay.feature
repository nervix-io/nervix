Feature: Web console visual relay and subscription creation

  @visual_relay_create
  Scenario Outline: Ordinary and materialized relays created visually keep their schema and branch identity
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE SCHEMA order_record (tenant STRING, amount I64);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='relay']" is clicked
    Then selector ".create-dialog" contains "Create relay"
    And selector ".create-scope" contains "{{domain}}"
    When selector ".create-name" is filled with "visual_orders"
    And selector ".create-schema-ref .create-choice-search" is filled with "order_record"
    Then selector ".create-schema-ref [data-value='order_record']" exists
    When selector ".create-schema-ref [data-value='order_record']" is clicked
    Then selector ".create-selected-schema" contains "order_record"
    When selector ".create-branching-options [data-value='BRANCHED BY']" is clicked
    And selector ".create-branch-ref .create-choice-search" is filled with "by_tenant"
    Then selector ".create-branch-ref [data-value='by_tenant']" exists
    When selector ".create-branch-ref [data-value='by_tenant']" is clicked
    Then selector ".create-selected-branch" contains "by_tenant"
    When selector ".create-capacity" is filled with "4"
    And selector ".create-materialized-options [data-value='LAST BY TIMESTAMP']" is clicked
    Then selector ".create-preview" contains "CREATE RELAY visual_orders SCHEMA order_record BRANCHED BY by_tenant CAPACITY 4 WITH MATERIALIZED STATE LAST BY TIMESTAMP;"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-name" is filled with "visual_audit"
    And selector ".create-branching-options [data-value='UNBRANCHED']" is clicked
    And selector ".create-materialized-options [data-value='NONE']" is clicked
    And selector ".create-capacity" is filled with "2"
    Then selector ".create-preview" contains "CREATE RELAY visual_audit SCHEMA order_record UNBRANCHED CAPACITY 2;"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE RELAY visual_orders;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE RELAY visual_orders SCHEMA order_record BRANCHED BY by_tenant CAPACITY 4 WITH MATERIALIZED STATE LAST BY TIMESTAMP;" exactly 2 times
    When selector ".prompt-row input" is filled with "SHOW CREATE RELAY visual_audit;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE RELAY visual_audit SCHEMA order_record UNBRANCHED CAPACITY 2;" exactly 2 times
    When selector ".prompt-row input" is filled with "DESCRIBE RELAY visual_orders;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "by_tenant"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_subscription_create
  Scenario Outline: A subscription created visually streams filtered interleaved branches in its tab
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE SCHEMA notification ( tenant STRING, user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( tenant string, user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_tenant;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR notifications_source FROM ENDPOINT notifications_endpoint MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec TO notifications INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='subscription']" is clicked
    Then selector ".create-dialog" contains "Create subscription"
    When selector ".create-name" is filled with "acme_notifications"
    And selector ".create-relay-ref .create-choice-search" is filled with "notifications"
    Then selector ".create-relay-ref [data-value='notifications']" exists
    When selector ".create-relay-ref [data-value='notifications']" is clicked
    Then selector ".create-selected-relay" contains "notifications"
    And selector ".create-field-refs [data-value='user_id']" contains "I64"
    When selector ".create-field-refs [data-value='tenant']" is clicked
    Then selector ".create-filter" has value "input.tenant"
    When selector ".create-filter" is filled with "input.tenant = 'acme'"
    And selector ".create-delivery-options [data-value='DROPPING']" is clicked
    And selector ".create-sample" is clicked
    And selector ".create-sample-rate" is filled with "1"
    Then selector ".create-preview" contains "CREATE SUBSCRIPTION acme_notifications TO notifications DROPPING BATCH SAMPLE RATE 1 WHERE input.tenant = 'acme';"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    And selector ".subscription-tab[data-subscription-name='acme_notifications'][data-subscription-state='active']" exists
    When selector ".create-close" is clicked
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","user_id":7}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":42}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","user_id":8}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":43}
      """
    Then selector ".terminal" contains '"user_id":42'
    And selector ".terminal" contains '"user_id":43'
    And selector ".terminal" contains '"user_id":42' exactly 1 times
    And selector ".terminal" does not contain "beta"
    When selector ".subscription-tab[data-subscription-name='acme_notifications'] .tab-close" is clicked
    Then selector ".subscription-tab[data-subscription-name='acme_notifications']" eventually disappears

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A visual subscription is restored after a leader change without a duplicate tab
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA metric ( value I32 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT raw_metrics_endpoint ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR raw_metrics_source FROM ENDPOINT raw_metrics_endpoint MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec TO raw_metrics INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='subscription']" is clicked
    And selector ".create-name" is filled with "restored_metrics"
    And selector ".create-relay-ref .create-choice-search" is filled with "raw_metrics"
    Then selector ".create-relay-ref [data-value='raw_metrics']" exists
    When selector ".create-relay-ref [data-value='raw_metrics']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    Then selector ".terminal" contains ":1}"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then selector ".terminal" contains "delivery interrupted"
    And selector ".subscription-tab[data-subscription-name='restored_metrics'][data-subscription-state='active']" exists
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    Then selector ".terminal" contains ":2}"
    And selector ".terminal" contains ":2}" exactly 1 times
    And selector ".subscription-tab" has at most 1 elements
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='subscription']" is clicked
    Then selector ".create-status" contains "Editing"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "restored_metrics"
    And selector ".subscription-tab" has at most 1 elements

  Scenario: Subscription statements typed in the REPL open and close tabs through the same lifecycle
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA metric ( value I64 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT raw_metrics_endpoint ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR raw_metrics_source FROM ENDPOINT raw_metrics_endpoint MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec TO raw_metrics INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "CREATE SUBSCRIPTION typed_metrics TO raw_metrics WHERE input.value > 1;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".subscription-tab[data-subscription-name='typed_metrics'][data-subscription-state='active']" exists
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    Then selector ".terminal" contains ":2}"
    And selector ".terminal" does not contain ":1}"
    When selector ".repl-toolbar button:has-text('NSPL REPL')" is clicked
    And selector ".prompt-row input" is filled with "DELETE SUBSCRIPTION typed_metrics;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".subscription-tab[data-subscription-name='typed_metrics']" eventually disappears
    When selector ".prompt-row input" is filled with "DELETE SUBSCRIPTION typed_metrics;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "no subscription tab named 'typed_metrics'"

  Scenario: Failed visual subscriptions keep their draft and open no tab
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY doomed_notifications SCHEMA notification UNBRANCHED;
      START;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='subscription']" is clicked
    And selector ".create-name" is filled with "typed_filter"
    And selector ".create-relay-ref .create-choice-search" is filled with "notifications"
    Then selector ".create-relay-ref [data-value='notifications']" exists
    When selector ".create-relay-ref [data-value='notifications']" is clicked
    And selector ".create-filter" is filled with "input.user_id = = 1"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Filter"
    And selector ".create-filter" has value "input.user_id = = 1"
    When selector ".create-filter" is filled with "input.user_id = 1"
    And selector ".create-sample" is clicked
    And selector ".create-sample-rate" is filled with "1.5"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Sample rate"
    When selector ".create-sample" is clicked
    And selector ".create-filter" is filled with "input.user_id = 'wrong type'"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "typed_filter"
    And selector ".create-filter" has value "input.user_id = 'wrong type'"
    And selector ".subscription-tab" does not exist
    When selector ".create-name" is filled with "stale_relay"
    And selector ".create-filter" is filled with ""
    And selector ".create-relay-ref .create-choice-search" is filled with "doomed_notifications"
    Then selector ".create-relay-ref [data-value='doomed_notifications']" exists
    When selector ".create-relay-ref [data-value='doomed_notifications']" is clicked
    Then selector ".create-selected-relay" contains "doomed_notifications"
    When these NSPL commands are executed on the leader node
      """
      DROP RELAY doomed_notifications;
      """
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "doomed_notifications"
    And selector ".create-error" contains "does not exist"
    And selector ".create-selected-relay" contains "doomed_notifications"
    And selector ".subscription-tab" does not exist

  Scenario: A relay staged in a transaction is selectable while its subscription is refused
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='relay']" is clicked
    And selector ".create-name" is filled with "staged_notifications"
    And selector ".create-schema-ref .create-choice-search" is filled with "notification"
    Then selector ".create-schema-ref [data-value='notification']" exists
    When selector ".create-schema-ref [data-value='notification']" is clicked
    And selector ".create-branching-options [data-value='UNBRANCHED']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    And selector ".create-status" contains "position 1"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='subscription']" is clicked
    And selector ".create-name" is filled with "staged_tab"
    And selector ".create-relay-ref .create-choice-search" is filled with "staged_notifications"
    Then selector ".create-relay-ref [data-value='staged_notifications']" exists
    When selector ".create-relay-ref [data-value='staged_notifications']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "cannot be queued in a transaction"
    And selector ".subscription-tab" does not exist
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "REVERT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears

  Scenario: Relay drafts keep unselected branching and changed references explicit
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE DOMAIN {{domain}}_other;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='relay']" is clicked
    And selector ".create-name" is filled with "retained_relay"
    And selector ".create-schema-ref .create-choice-search" is filled with "tenant_key"
    Then selector ".create-schema-ref [data-value='tenant_key']" exists
    When selector ".create-schema-ref [data-value='tenant_key']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Choose UNBRANCHED or a branch"
    When selector ".create-branching-options [data-value='BRANCHED BY']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Choose a branch"
    When selector ".create-branch-ref .create-choice-search" is filled with "by_tenant"
    Then selector ".create-branch-ref [data-value='by_tenant']" exists
    When selector ".create-branch-ref [data-value='by_tenant']" is clicked
    And selector ".create-capacity" is filled with "0"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Capacity"
    When selector ".create-capacity" is filled with "3"
    And selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='relay']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    And selector ".create-scope-change" exists
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{domain}}_other"
    And selector ".create-selected-schema" contains "tenant_key"
    And selector ".create-selected-branch" contains "by_tenant"
    And selector ".create-reference-invalid" exists
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "belongs to a changed context"
    And selector ".create-name" has value "retained_relay"
    And selector ".create-capacity" has value "3"

  Scenario: Relay and subscription forms without a domain stay local
    Given a 1 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='relay']" is clicked
    Then selector ".create-scope" contains "No domain selected"
    And selector ".create-schema-ref" contains "Select a domain"
    When selector ".create-name" is filled with "orphan_relay"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Select a domain"
    And selector ".terminal" does not contain "CREATE RELAY orphan_relay"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='subscription']" is clicked
    Then selector ".create-scope" contains "No domain selected"
    And selector ".create-relay-ref" contains "Select a domain"
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Select a domain"
    And selector ".subscription-tab" does not exist

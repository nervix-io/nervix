Feature: Web console visual client, vhost and endpoint creation

  @visual_client_endpoint_create
  Scenario Outline: A visual VHOST and HTTP endpoint serve a running ingestor
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_payload (message STRING);
      CREATE WIRE JSON SCHEMA visual_payload_wire MODE STRICT (message string);
      CREATE CODEC visual_payload_codec FROM WIRE JSON SCHEMA visual_payload_wire TO SCHEMA visual_payload;
      CREATE RELAY visual_inbox SCHEMA visual_payload UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='vhost']" is clicked
    And selector ".create-name" is filled with "visual_edge"
    And selector ".create-vhost-hostname" is filled with "visual-{{test_id}}.example.com"
    Then selector ".create-preview" contains "CREATE VHOST visual_edge visual-{{test_id}}.example.com"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='endpoint']" is clicked
    And selector ".create-name" is filled with "visual_ingest"
    And selector ".create-endpoint-vhost .create-choice-search" is filled with "visual_edge"
    Then selector ".create-endpoint-vhost [data-value='visual_edge']" exists
    When selector ".create-endpoint-vhost [data-value='visual_edge']" is clicked
    And selector ".create-endpoint-path" is filled with "/ingest"
    And selector ".create-endpoint-type [data-type='http']" is clicked
    Then selector ".create-preview" contains "CREATE ENDPOINT visual_ingest ON visual_edge PATH '/ingest' TYPE HTTP"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE ENDPOINT visual_ingest;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE ENDPOINT visual_ingest ON visual_edge PATH '/ingest' TYPE HTTP"
    When these NSPL commands are executed on the leader node
      """
      CREATE INGESTOR visual_source FROM ENDPOINT visual_ingest MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_payload_codec TO visual_inbox INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "visual-{{test_id}}.example.com" path "/ingest"
      """
      {"message":"from visual endpoint"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_client_pool_create
  Scenario: A pooled client preserves its explicit bounds and sends masked configuration values
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='client']" is clicked
    And selector ".create-name" is filled with "visual_redis"
    And selector ".create-client-type [data-type='http']" is clicked
    And selector ".create-client-config-add" is clicked
    And selector ".create-client-type [data-type='redis']" is clicked
    Then selector ".create-client-config-entry" does not exist
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Pool minimum"
    When selector ".create-client-pool-min" is filled with "5"
    And selector ".create-client-pool-max" is filled with "4"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "minimum pool size 5 must not exceed maximum pool size 4"
    When selector ".create-client-pool-min" is filled with "1"
    And selector ".create-client-config-add" is clicked
    And selector ".create-client-config-entry:nth-of-type(1) .create-client-config-key" is filled with "addr"
    And selector ".create-client-config-entry:nth-of-type(1) .create-client-config-value" is filled with "redis://127.0.0.1:6379/"
    And selector ".create-client-config-add" is clicked
    And selector ".create-client-config-entry:nth-of-type(2) .create-client-config-key" is filled with "password"
    And selector ".create-client-config-entry:nth-of-type(2) .create-client-config-value" is filled with "visual-secret"
    Then selector ".create-preview" contains "POOL SIZE MIN 1 MAX 4"
    And selector ".create-preview" does not contain "visual-secret"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE CLIENT visual_redis;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "POOL SIZE MIN 1 MAX 4"
    And selector ".terminal" contains "visual-secret"

  @visual_client_mount_create
  Scenario: A client mount selects an explicit completed resource version
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "client_mount" containing
      """
      {"ca.pem":"sample material"}
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE visual_client_bundle;
      UPLOAD RESOURCE visual_client_bundle VERSION '{{client_mount}}';
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='client']" is clicked
    And selector ".create-name" is filled with "visual_http"
    And selector ".create-client-type [data-type='http']" is clicked
    And selector ".create-client-mount-enabled" is clicked
    And selector ".create-client-resource .create-choice-search" is filled with "visual_client_bundle"
    Then selector ".create-client-resource [data-value='visual_client_bundle']" exists
    When selector ".create-client-resource [data-value='visual_client_bundle']" is clicked
    Then selector ".create-client-version [data-value='1']" exists
    When selector ".create-client-version [data-value='1']" is clicked
    And selector ".create-client-config-add" is clicked
    And selector ".create-client-config-key" is filled with "url"
    And selector ".create-client-config-value" is filled with "http://127.0.0.1:1"
    Then selector ".create-preview" contains "MOUNT visual_client_bundle VERSION 1"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE CLIENT visual_http;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "MOUNT visual_client_bundle VERSION 1"

  @visual_vhost_tls_create
  Scenario Outline: A visual TLS VHOST installs its pinned certificate on every live node
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has TLS resource directory "visual_tls" for hosts "tls-{{test_id}}.example.com"
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE visual_tls_bundle;
      UPLOAD RESOURCE visual_tls_bundle VERSION '{{visual_tls}}';
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='vhost']" is clicked
    And selector ".create-name" is filled with "visual_tls_edge"
    And selector ".create-vhost-hostname" is filled with "tls-{{test_id}}.example.com"
    And selector ".create-vhost-tls-enabled" is clicked
    And selector ".create-vhost-resource .create-choice-search" is filled with "visual_tls_bundle"
    Then selector ".create-vhost-resource [data-value='visual_tls_bundle']" exists
    When selector ".create-vhost-resource [data-value='visual_tls_bundle']" is clicked
    Then selector ".create-vhost-version [data-value='1']" exists
    When selector ".create-vhost-version [data-value='1']" is clicked
    Then selector ".create-preview" contains "WITH TLS visual_tls_bundle VERSION 1"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    And the HTTPS listener of every node for host "tls-{{test_id}}.example.com" presents the certificate from resource directory "visual_tls"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_websocket_signaling_create
  Scenario: WebSocket client and endpoint select an existing signaling protocol
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SIGNALING PROTOCOL visual_handshake FORMAT JSON ON CONNECT SEND JAQ '{hello: true}' WAIT JAQ '.ready' TIMEOUT 5s;
      CREATE VHOST visual_edge ws-{{test_id}}.example.com;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='client']" is clicked
    And selector ".create-name" is filled with "visual_socket_client"
    And selector ".create-client-type [data-type='websockets']" is clicked
    And selector ".create-client-signaling .create-choice-search" is filled with "visual_handshake"
    Then selector ".create-client-signaling [data-value='visual_handshake']" exists
    When selector ".create-client-signaling [data-value='visual_handshake']" is clicked
    And selector ".create-client-config-add" is clicked
    And selector ".create-client-config-key" is filled with "endpoint"
    And selector ".create-client-config-value" is filled with "ws://example.com/socket"
    Then selector ".create-preview" contains "WITH SIGNALING PROTOCOL visual_handshake"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='endpoint']" is clicked
    And selector ".create-name" is filled with "visual_socket_endpoint"
    And selector ".create-endpoint-vhost .create-choice-search" is filled with "visual_edge"
    Then selector ".create-endpoint-vhost [data-value='visual_edge']" exists
    When selector ".create-endpoint-vhost [data-value='visual_edge']" is clicked
    And selector ".create-endpoint-path" is filled with "/socket"
    And selector ".create-endpoint-type [data-type='websockets']" is clicked
    And selector ".create-endpoint-signaling .create-choice-search" is filled with "visual_handshake"
    Then selector ".create-endpoint-signaling [data-value='visual_handshake']" exists
    When selector ".create-endpoint-signaling [data-value='visual_handshake']" is clicked
    Then selector ".create-preview" contains "TYPE WEBSOCKETS WITH SIGNALING PROTOCOL visual_handshake"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE ENDPOINT visual_socket_endpoint;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "TYPE WEBSOCKETS WITH SIGNALING PROTOCOL visual_handshake"

  @visual_endpoint_transaction
  Scenario: An endpoint can select a VHOST staged earlier in its transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".prompt-row input" is filled with "CREATE VHOST staged_edge staged-{{test_id}}.example.com;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='endpoint']" is clicked
    And selector ".create-name" is filled with "staged_endpoint"
    And selector ".create-endpoint-vhost .create-choice-search" is filled with "staged_edge"
    Then selector ".create-endpoint-vhost [data-value='staged_edge']" exists
    When selector ".create-endpoint-vhost [data-value='staged_edge']" is clicked
    And selector ".create-endpoint-path" is filled with "/staged"
    And selector ".create-endpoint-type [data-type='http']" is clicked
    And selector ".create-submit" is clicked
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
    When selector ".prompt-row input" is filled with "SHOW CREATE ENDPOINT staged_endpoint;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE ENDPOINT staged_endpoint ON staged_edge PATH '/staged' TYPE HTTP"

  @visual_endpoint_reconnect
  Scenario: Selected endpoint references survive a leader reconnection
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE VHOST reconnect_edge reconnect-{{test_id}}.example.com;
      CREATE SIGNALING PROTOCOL reconnect_handshake FORMAT JSON ON CONNECT SEND JAQ '{hello: true}' WAIT JAQ '.ready' TIMEOUT 5s;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='endpoint']" is clicked
    And selector ".create-name" is filled with "reconnected_endpoint"
    And selector ".create-endpoint-vhost .create-choice-search" is filled with "reconnect_edge"
    Then selector ".create-endpoint-vhost [data-value='reconnect_edge']" exists
    When selector ".create-endpoint-vhost [data-value='reconnect_edge']" is clicked
    And selector ".create-endpoint-path" is filled with "/reconnected"
    And selector ".create-endpoint-type [data-type='websockets']" is clicked
    And selector ".create-endpoint-signaling .create-choice-search" is filled with "reconnect_handshake"
    Then selector ".create-endpoint-signaling [data-value='reconnect_handshake']" exists
    When selector ".create-endpoint-signaling [data-value='reconnect_handshake']" is clicked
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".create-preview" contains "TYPE WEBSOCKETS WITH SIGNALING PROTOCOL reconnect_handshake"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE ENDPOINT reconnected_endpoint;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE ENDPOINT reconnected_endpoint ON reconnect_edge"

Feature: Web console visual codec and signaling protocol creation

  @visual_codec_create
  Scenario Outline: A schema codec created from exact wire and internal schema choices is usable
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA visual_payload (message STRING);
      CREATE WIRE JSON SCHEMA visual_payload_wire MODE STRICT (message string);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='codec']" is clicked
    And selector ".create-name" is filled with "visual_payload_codec"
    And selector ".create-codec-format [data-format='wire-json']" is clicked
    And selector ".create-codec-schema .create-choice-search" is filled with "visual_payload"
    Then selector ".create-codec-schema [data-value='visual_payload']" exists
    When selector ".create-codec-schema [data-value='visual_payload']" is clicked
    And selector ".create-codec-wire-schema .create-choice-search" is filled with "visual_payload_wire"
    Then selector ".create-codec-wire-schema [data-value='visual_payload_wire']" exists
    When selector ".create-codec-wire-schema [data-value='visual_payload_wire']" is clicked
    Then selector ".create-preview" contains "CREATE CODEC visual_payload_codec"
    And selector ".create-preview" contains "FROM WIRE JSON SCHEMA visual_payload_wire"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE CODEC visual_payload_codec;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE CODEC visual_payload_codec"
    And selector ".terminal" contains "FROM WIRE JSON SCHEMA visual_payload_wire"
    When these NSPL commands are executed on the leader node
      """
      CREATE RELAY visual_inbox SCHEMA visual_payload UNBRANCHED;
      CREATE VHOST visual_edge http-{{test_id}}.example.com;
      CREATE ENDPOINT visual_ingest ON visual_edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR visual_source FROM ENDPOINT visual_ingest MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_payload_codec TO visual_inbox INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_signaling_create
  Scenario: A multi-step signaling protocol retains its order and program text
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='signaling-protocol']" is clicked
    And selector ".create-name" is filled with "visual_handshake"
    And selector ".create-signaling-format [data-format='json']" is clicked
    And selector ".create-signaling-add-send" is clicked
    And selector ".create-signaling-step:nth-of-type(1) .create-signaling-send-program" is filled with "{id: 1}"
    And selector ".create-signaling-add-wait" is clicked
    And selector ".create-signaling-step:nth-of-type(2) .create-signaling-wait-matcher" is filled with ".id == 1"
    And selector ".create-signaling-step:nth-of-type(2) .create-signaling-capture-enabled" is clicked
    And selector ".create-signaling-step:nth-of-type(2) .create-signaling-capture" is filled with "{token: .token}"
    And selector ".create-signaling-step:nth-of-type(2) .create-signaling-wait-fail-list .create-signaling-program-add" is clicked
    And selector ".create-signaling-step:nth-of-type(2) .create-signaling-wait-fail" is filled with ".error"
    And selector ".create-signaling-fail-list .create-signaling-program-add" is clicked
    And selector ".create-signaling-fail" is filled with ".fatal"
    And selector ".create-signaling-step:nth-of-type(2) .create-signaling-accept-data" is clicked
    And selector ".create-signaling-timeout" is filled with "5s"
    Then selector ".create-preview" contains "SEND JAQ"
    And selector ".create-preview" contains "WAIT JAQ"
    And selector ".create-preview" contains "CAPTURE"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE SIGNALING PROTOCOL visual_handshake;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SIGNALING PROTOCOL visual_handshake"
    And selector ".terminal" contains "WAIT JAQ"
    When these NSPL commands are executed on the leader node
      """
      CREATE VHOST visual_edge ws-{{test_id}}.example.com;
      CREATE ENDPOINT visual_socket ON visual_edge PATH '/ws' TYPE WEBSOCKETS WITH SIGNALING PROTOCOL visual_handshake;
      """

  @visual_jaq_codec_create
  Scenario: A jaq codec keeps decode, encode and batch programs through canonical creation
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA visual_payload (message STRING);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='codec']" is clicked
    And selector ".create-name" is filled with "visual_jaq_codec"
    And selector ".create-codec-format [data-format='json']" is clicked
    And selector ".create-codec-schema .create-choice-search" is filled with "visual_payload"
    Then selector ".create-codec-schema [data-value='visual_payload']" exists
    When selector ".create-codec-schema [data-value='visual_payload']" is clicked
    And selector ".create-codec-ingestion-enabled" is clicked
    And selector ".create-codec-ingestion-program" is filled with "."
    And selector ".create-codec-emitting-enabled" is clicked
    And selector ".create-codec-emitting-program" is filled with "{message: .message}"
    And selector ".create-codec-batch-enabled" is clicked
    And selector ".create-codec-batch-program" is filled with "{records: .}"
    Then selector ".create-preview" contains "ON EMITTING BATCH"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE CODEC visual_jaq_codec;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "ON INGESTION"
    And selector ".terminal" contains "ON EMITTING BATCH"

  @visual_protobuf_create
  Scenario: A resource-backed codec selects a completed version and file explicitly
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "visual_proto" containing
      """
      {
        "payload.proto": "syntax = \"proto3\"; package visual; message Payload { string message = 1; }"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA visual_payload (message STRING);
      CREATE RESOURCE visual_proto_bundle;
      UPLOAD RESOURCE visual_proto_bundle VERSION '{{visual_proto}}';
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='codec']" is clicked
    And selector ".create-name" is filled with "visual_proto_codec"
    And selector ".create-codec-format [data-format='protobuf']" is clicked
    And selector ".create-codec-schema .create-choice-search" is filled with "visual_payload"
    Then selector ".create-codec-schema [data-value='visual_payload']" exists
    When selector ".create-codec-schema [data-value='visual_payload']" is clicked
    And selector ".create-protobuf-resource .create-choice-search" is filled with "visual_proto_bundle"
    Then selector ".create-protobuf-resource [data-value='visual_proto_bundle']" exists
    When selector ".create-protobuf-resource [data-value='visual_proto_bundle']" is clicked
    Then selector ".create-protobuf-version [data-value='LATEST']" exists
    When selector ".create-protobuf-version [data-value='LATEST']" is clicked
    And selector ".create-protobuf-file" is filled with "payload.proto"
    And selector ".create-codec-message" is filled with "visual.Payload"
    And selector ".create-codec-ingestion-enabled" is clicked
    And selector ".create-codec-ingestion-program" is filled with "."
    Then selector ".create-preview" contains "VERSION LATEST"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE CODEC visual_proto_codec;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "VERSION 1"
    And selector ".terminal" contains "payload.proto"

  @visual_invalid_codec
  Scenario: An invalid jaq program keeps the editable draft and never reports completion
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA visual_payload (message STRING);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='codec']" is clicked
    And selector ".create-name" is filled with "visual_invalid_codec"
    And selector ".create-codec-format [data-format='json']" is clicked
    And selector ".create-codec-schema .create-choice-search" is filled with "visual_payload"
    Then selector ".create-codec-schema [data-value='visual_payload']" exists
    When selector ".create-codec-schema [data-value='visual_payload']" is clicked
    And selector ".create-codec-ingestion-enabled" is clicked
    And selector ".create-codec-ingestion-program" is filled with "["
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "invalid ingestion jaq transformation"
    And selector ".create-codec-ingestion-program" has value "["
    When selector ".create-codec-ingestion-program" is filled with "."
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"

  @visual_codec_transaction
  Scenario: A codec can choose a wire schema staged earlier in its transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA visual_payload (message STRING);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".prompt-row input" is filled with "CREATE WIRE JSON SCHEMA staged_wire MODE STRICT (message string);"
    And selector ".prompt-row input" is pressed with "Enter"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='codec']" is clicked
    And selector ".create-name" is filled with "staged_codec"
    And selector ".create-codec-format [data-format='wire-json']" is clicked
    And selector ".create-codec-schema .create-choice-search" is filled with "visual_payload"
    Then selector ".create-codec-schema [data-value='visual_payload']" exists
    When selector ".create-codec-schema [data-value='visual_payload']" is clicked
    And selector ".create-codec-wire-schema .create-choice-search" is filled with "staged_wire"
    Then selector ".create-codec-wire-schema [data-value='staged_wire']" exists
    When selector ".create-codec-wire-schema [data-value='staged_wire']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    And selector ".create-status" contains "position 2"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='signaling-protocol']" is clicked
    And selector ".create-name" is filled with "staged_handshake"
    And selector ".create-signaling-format [data-format='json']" is clicked
    And selector ".create-signaling-add-send" is clicked
    And selector ".create-signaling-step:nth-of-type(1) .create-signaling-send-program" is filled with "{id: 1}"
    And selector ".create-signaling-add-wait" is clicked
    And selector ".create-signaling-step:nth-of-type(2) .create-signaling-wait-matcher" is filled with ".id == 1"
    And selector ".create-signaling-timeout" is filled with "5s"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    And selector ".create-status" contains "position 3"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "3 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "COMPLETE"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector ".prompt-row input" is filled with "SHOW CREATE CODEC staged_codec;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "FROM WIRE JSON SCHEMA staged_wire"
    When selector ".prompt-row input" is filled with "SHOW CREATE SIGNALING PROTOCOL staged_handshake;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE SIGNALING PROTOCOL staged_handshake"

  @visual_codec_reconnect
  Scenario: Selected codec references survive a leader reconnection
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA visual_payload (message STRING);
      CREATE WIRE JSON SCHEMA visual_payload_wire MODE STRICT (message string);
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='codec']" is clicked
    And selector ".create-name" is filled with "reconnected_codec"
    And selector ".create-codec-format [data-format='wire-json']" is clicked
    And selector ".create-codec-schema .create-choice-search" is filled with "visual_payload"
    Then selector ".create-codec-schema [data-value='visual_payload']" exists
    When selector ".create-codec-schema [data-value='visual_payload']" is clicked
    And selector ".create-codec-wire-schema .create-choice-search" is filled with "visual_payload_wire"
    Then selector ".create-codec-wire-schema [data-value='visual_payload_wire']" exists
    When selector ".create-codec-wire-schema [data-value='visual_payload_wire']" is clicked
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".create-preview" contains "FROM WIRE JSON SCHEMA visual_payload_wire"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE CODEC reconnected_codec;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE CODEC reconnected_codec"

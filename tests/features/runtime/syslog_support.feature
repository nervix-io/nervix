Feature: Syslog support
  Scenario Outline: Syslog UDP intake decodes and forwards RFC 5424 messages
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_event (
        facility U8,
        severity U8,
        timestamp DATETIME OPTIONAL,
        hostname STRING OPTIONAL,
        app_name STRING OPTIONAL,
        proc_id STRING OPTIONAL,
        msg_id STRING OPTIONAL,
        structured_data STRING OPTIONAL,
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY syslog_events SCHEMA syslog_event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_ingest_addr}}'
        };
      CREATE SYSLOG CLIENT syslog_forwarder
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_emit_addr}}'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SYSLOG COLLECTOR EMITTER syslog_forward
        FROM syslog_events
        TO syslog_forwarder
          MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    When these Plexus commands are executed on the active session
      """
      SHOW CREATE CODEC syslog_codec;
      """
    Then the last command output contains
      """
      CREATE CODEC syslog_codec
        FROM SYSLOG
        TO SCHEMA syslog_event;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    And within "5s" DESCRIBE EMITTER "syslog_forward" on the leader node contains
      """
      transient error: -
      reconnect backoff: -
      reconnect wait: -
      """
    And the last command output contains
      """
      to: SYSLOG COLLECTOR syslog_forwarder
      options: none
      mode: NO ACK RETRY POLICY BACKOFF 50ms MAX 1s
      quiesce: none
      envelope: none
      batch: none
      flush: FLUSH IMMEDIATE
      errors: ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
      from: syslog_events
      codec: syslog_codec
      body: codec
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 [exampleSDID@32473 iut="3"] order accepted
      """
    Then the relay subscription receives a payload
      """
      "facility":4
      """
    And the last relay subscription payload contains
      """
      "severity":2
      "hostname":"edge-1"
      "app_name":"orders"
      "proc_id":"123"
      "msg_id":"ID47"
      "structured_data":"[exampleSDID@32473 iut=\"3\"]"
      "message":"order accepted"
      """
    And the observed Syslog UDP endpoint receives a payload
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 [exampleSDID@32473 iut="3"] order accepted
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario: A Syslog subscription survives a leader change before inspecting its codec
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_event (facility U8, severity U8, message STRING);
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY syslog_events SCHEMA syslog_event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener CONFIG {
        'protocol' = 'udp',
        'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then the current leader node is saved as placeholder "subscribed_leader"
    And a node other than placeholder "subscribed_leader" is saved as placeholder "successor"
    When leadership is transferred from node "{{subscribed_leader}}" to node "{{successor}}"
    Then node "{{subscribed_leader}}" eventually reports leader "{{successor}}"
    When these Plexus commands are executed on the active session
      """
      SHOW CREATE CODEC syslog_codec;
      """
    Then the last command output contains
      """
      CREATE CODEC syslog_codec
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      ready: true
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      <34>1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 - after leadership change
      """
    Then the relay subscription receives a payload
      """
      "facility":4
      """
    And the last relay subscription payload contains
      """
      "severity":2
      "message":"after leadership change"
      """

  Scenario: Syslog UDP listeners follow node lifecycle, leadership, and membership
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_event (
        facility U8,
        severity U8,
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY syslog_events SCHEMA syslog_event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_ingest_addr}}'
        };
      CREATE SYSLOG CLIENT syslog_forwarder
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_emit_addr}}'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SYSLOG COLLECTOR EMITTER syslog_forward
        FROM syslog_events
        TO syslog_forwarder
          MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually forwards Syslog UDP message "initial-node-1" at "{{syslog_ingest_addr}}" to the observed endpoint
    And node "node-2" eventually forwards Syslog UDP message "initial-node-2" at "{{syslog_ingest_addr}}" to the observed endpoint
    And node "node-3" eventually forwards Syslog UDP message "initial-node-3" at "{{syslog_ingest_addr}}" to the observed endpoint
    Then node "node-1" eventually reports leader "node-1"
    When leadership is transferred from node "node-1" to node "node-2"
    Then node "node-1" eventually reports leader "node-2"
    And node "node-1" eventually forwards Syslog UDP message "after-leadership-change" at "{{syslog_ingest_addr}}" to the observed endpoint
    When node "node-3" is stopped
    And node "node-3" is started
    Then node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-3" eventually reports interconnect to "node-1" as "connected"
    And node "node-3" eventually reports interconnect to "node-2" as "connected"
    And node "node-3" eventually forwards Syslog UDP message "after-node-restart" at "{{syslog_ingest_addr}}" to the observed endpoint
    When node "node-4" is added to the cluster
    Then node "node-1" eventually reports interconnect to "node-4" as "connected"
    And node "node-4" eventually forwards Syslog UDP message "after-node-join" at "{{syslog_ingest_addr}}" to the observed endpoint

  Scenario Outline: A Syslog TLS listener whose key file holds no key names the connector failure at startup
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
    When these Plexus commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT invalid_syslog_listener
        MOUNT syslog_tls VERSION 1
        CONFIG {
          'protocol' = 'tls',
          'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.crt'
        };
      CREATE SYSLOG LISTENER INGESTOR invalid_syslog_intake
        FROM invalid_syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "failed to initialize ingestor 'invalid_syslog_intake' in domain '{{domain}}'"
      """
      START;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Syslog TLS intake uses RFC 5425 framing and mounted mutual TLS identity
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
    When these Plexus commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        MOUNT syslog_tls VERSION 1
        CONFIG {
          'protocol' = 'tls',
          'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.key',
          'tls_client_ca_file' = '{{ syslog_tls }}/ca.crt'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When Syslog TLS message is published to "{{syslog_ingest_addr}}" using identity and CA from resource directory "syslog_tls_dir"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-3 secure 789 ID49 - tls framed
      """
    Then the relay subscription receives a payload
      """
      "message":"tls framed"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Syslog TCP intake accepts mixed RFC 6587 framing and exposes peer metadata
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE SCHEMA received_syslog_message (
        message STRING,
        peer_addr STRING OPTIONAL
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA received_syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'tcp',
          'addr' = '{{syslog_ingest_addr}}',
          'max_message_size' = '4096'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT message
          SET peer_addr = metadata.peer_addr
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When Syslog TCP messages are published with mixed framing to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}Oct 11 22:14:15 edge-2 worker: octet counted
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-2 worker 456 ID48 - line framed
      """
    Then the relay subscription receives a payload
      """
      "message":"octet counted"
      """
    And the last relay subscription payload contains
      """
      "peer_addr":"127.0.0.1:
      """
    Then the relay subscription receives a payload
      """
      "message":"line framed"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @syslog_suspend_handoff @deloxide_stress
  Scenario Outline: A full Syslog TCP intake keeps every frame through source suspension on <cluster_size> nodes via <node_id>
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (message STRING);
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE CODEC syslog_alternate_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED CAPACITY 4;
      CREATE SYSLOG CLIENT syslog_listener CONFIG {
        'protocol' = 'tcp',
        'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Given ingestor "syslog_intake" pauses inside dispatch
    And the entity gate for domain "{{domain}}" pauses after engagement
    When a Syslog TCP sender publishes 80 numbered frames to node "<node_id>" at "{{syslog_ingest_addr}}"
    Then ingestor "syslog_intake" reaches the dispatch pause
    When these Plexus commands begin executing in the background
      """
      ALTER SYSLOG LISTENER INGESTOR syslog_intake SET DECODE USING syslog_alternate_codec;
      """
    Then within "10s" node "<node_id>" eventually reports describe ingestor "syslog_intake" as "status: quiesced"
    When ingestor "syslog_intake" leaves the dispatch pause
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "10s" node "<node_id>" eventually reports describe ingestor "syslog_intake" as "ready: false"
    # End the alteration before it replaces the source, so the held source resumes.
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution fails with "timed out draining domain"
    And within "30s" the relay subscription receives 80 numbered Syslog frames once
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count | node_id |
      | 1            | 0             | node-1  |
      | 3            | 1             | node-1  |
      | 3            | 1             | node-2  |
      | 3            | 1             | node-3  |

  Scenario: A forced drain refusal stays a Syslog suspension alteration's outcome when a new leader runs it again
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (message STRING);
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE CODEC syslog_alternate_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED CAPACITY 4;
      CREATE SYSLOG CLIENT syslog_listener CONFIG {
        'protocol' = 'tcp',
        'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given ingestor "syslog_intake" pauses inside dispatch
    And the entity gate for domain "{{domain}}" pauses after engagement
    When a Syslog TCP sender publishes 80 numbered frames to node "{{old_leader}}" at "{{syslog_ingest_addr}}"
    Then ingestor "syslog_intake" reaches the dispatch pause
    When these Plexus commands begin executing in the background
      """
      ALTER SYSLOG LISTENER INGESTOR syslog_intake SET DECODE USING syslog_alternate_codec;
      """
    Then within "10s" node "{{old_leader}}" eventually reports describe ingestor "syslog_intake" as "status: quiesced"
    When ingestor "syslog_intake" leaves the dispatch pause
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "10s" node "{{old_leader}}" eventually reports describe ingestor "syslog_intake" as "ready: false"
    # Leadership moves before the former leader records the refusal, so the command runs again
    # under its execution reference. The new leader's run waits until the first run has refused.
    Given transaction commit on node "{{new_leader}}" pauses after 0 statement
    When leadership is transferred to node "{{new_leader}}"
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When the entity gate pause for domain "{{domain}}" is released
    Then within "10s" node "{{old_leader}}" eventually reports describe ingestor "syslog_intake" as "ready: true"
    And the transaction commit pause on node "{{new_leader}}" after 0 statement is reached
    When the transaction commit pause on node "{{new_leader}}" after 0 statement is released
    Then the background Plexus execution fails with "timed out draining domain"
    And within "30s" the relay subscription receives 80 numbered Syslog frames once
    And the relay subscription does not receive a payload within "1s"

  @syslog_listener_rebind @deloxide_stress
  Scenario Outline: A Syslog accept error keeps an established TCP connection on <cluster_size> nodes via <node_id>
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (message STRING);
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener CONFIG {
        'protocol' = 'tcp',
        'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    When a live Syslog TCP sender connects to node "<node_id>" at "{{syslog_ingest_addr}}"
    And the live Syslog TCP sender writes message "before-error"
    Then within "10s" the relay subscription receives a payload
      """
      "message":"before-error"
      """
    When the Syslog listener on node "<node_id>" at "{{syslog_ingest_addr}}" fails its next accept through the fault hook
    Then the Syslog listener accept fault fires
    When the live Syslog TCP sender writes message "after-error"
    Then within "10s" the relay subscription receives a payload
      """
      "message":"after-error"
      """
    When a fresh Syslog TCP sender writes message "after-rebind" to node "<node_id>" at "{{syslog_ingest_addr}}"
    Then within "10s" the relay subscription receives a payload
      """
      "message":"after-rebind"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count | node_id |
      | 1            | 0             | node-1  |
      | 3            | 1             | node-1  |
      | 3            | 1             | node-2  |
      | 3            | 1             | node-3  |

  Scenario Outline: Syslog TCP intake reassembles a <framing> frame split across two reads
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'tcp',
          'addr' = '{{syslog_ingest_addr}}'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When a Syslog TCP sender writes to "{{syslog_ingest_addr}}" one <framing> frame and another split "<split>"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-4 split 101 ID50 - before the split
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-4 split 102 ID51 - across the split
      """
    Then within "10s" the relay subscription receives payloads in order
      """
      "message":"before the split"
      """
    When the Syslog sender writes the rest of the split frame
    Then within "10s" the relay subscription receives payloads in order
      """
      "message":"across the split"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count | framing         | split                  |
      | 1            | 0             | octet-counted   | inside its octet count |
      | 3            | 1             | octet-counted   | inside its payload     |
      | 1            | 0             | non-transparent | between CR and LF      |
      | 3            | 1             | non-transparent | inside its payload     |

  Scenario Outline: A Syslog TCP listener declaring octet-counting closes a connection that sends another framing
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'tcp',
          'addr' = '{{syslog_ingest_addr}}',
          'framing' = 'octet-counting'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When these Syslog TCP non-transparent frames are published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-6 framed 301 ID60 - line framed
      """
    And these Syslog TCP octet-counted frames are published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-6 framed 302 ID61 - octet counted
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "message":"octet counted"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A Syslog TCP listener declaring non-transparent framing reads a line that begins with a digit
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'tcp',
          'addr' = '{{syslog_ingest_addr}}',
          'framing' = 'non-transparent'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When these Syslog TCP non-transparent frames are published to "{{syslog_ingest_addr}}"
      """
      2003 digits lead this line
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-6 framed 303 ID62 - the following line
      """
    Then within "30s" the relay subscription receives payloads in order
      """
      "message":"2003 digits lead this line"
      "message":"the following line"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A Syslog TCP listener closes a connection whose frame exceeds max_message_size
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'tcp',
          'addr' = '{{syslog_ingest_addr}}',
          'max_message_size' = '512'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When a Syslog TCP sender writes a 513 byte frame and then this message to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-6 bounded 304 ID63 - after the oversize frame
      """
    And these Syslog TCP octet-counted frames are published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-6 bounded 305 ID64 - on a new connection
      """
    Then the relay subscription receives a payload
      """
      "message":"on a new connection"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A Syslog TLS listener with tls_client_ca_file refuses a sender that presents no certificate
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
    When these Plexus commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        MOUNT syslog_tls VERSION 1
        CONFIG {
          'protocol' = 'tls',
          'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.key',
          'tls_client_ca_file' = '{{ syslog_tls }}/ca.crt'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When a Syslog TLS sender trusting resource directory "syslog_tls_dir" presents no certificate to "{{syslog_ingest_addr}}" and is refused
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-7 secure 401 ID70 - no certificate
      """
    And Syslog TLS message is published to "{{syslog_ingest_addr}}" using identity and CA from resource directory "syslog_tls_dir"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-7 secure 402 ID71 - with a certificate
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "message":"with a certificate"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A Syslog TLS listener without tls_client_ca_file accepts a sender that presents no certificate
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
    When these Plexus commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        MOUNT syslog_tls VERSION 1
        CONFIG {
          'protocol' = 'tls',
          'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.key'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When a Syslog TLS sender trusting resource directory "syslog_tls_dir" presents no certificate to "{{syslog_ingest_addr}}" and is accepted
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-7 secure 403 ID72 - no certificate
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "message":"no certificate"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Syslog TLS intake reassembles an octet-counted frame split across two reads
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
    When these Plexus commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        MOUNT syslog_tls VERSION 1
        CONFIG {
          'protocol' = 'tls',
          'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.key',
          'tls_client_ca_file' = '{{ syslog_tls }}/ca.crt'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When a Syslog TLS sender using identity and CA from resource directory "syslog_tls_dir" writes to "{{syslog_ingest_addr}}" one octet-counted frame and another split "<split>"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-5 split 201 ID52 - before the split
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-5 split 202 ID53 - across the split
      """
    Then within "10s" the relay subscription receives payloads in order
      """
      "message":"before the split"
      """
    When the Syslog sender writes the rest of the split frame
    Then within "10s" the relay subscription receives payloads in order
      """
      "message":"across the split"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count | split                  |
      | 1            | 0             | inside its octet count |
      | 3            | 1             | inside its payload     |

  Scenario Outline: A Syslog TLS listener closes a client that stalls its handshake within connect_timeout_ms
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
    When these Plexus commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        MOUNT syslog_tls VERSION 1
        CONFIG {
          'protocol' = 'tls',
          'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.key',
          'tls_client_ca_file' = '{{ syslog_tls }}/ca.crt',
          'connect_timeout_ms' = '1000'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When a Syslog client that sends nothing connects to "{{syslog_ingest_addr}}"
    And Syslog TLS message is published to "{{syslog_ingest_addr}}" using identity and CA from resource directory "syslog_tls_dir"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-3 secure 789 ID49 - beside a stalled handshake
      """
    Then the relay subscription receives a payload
      """
      "message":"beside a stalled handshake"
      """
    And the Syslog client that sends nothing is disconnected no sooner than "1s" after it connected
    When Syslog TLS message is published to "{{syslog_ingest_addr}}" using identity and CA from resource directory "syslog_tls_dir"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-3 secure 790 ID50 - after a stalled handshake
      """
    Then the relay subscription receives a payload
      """
      "message":"after a stalled handshake"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A Syslog TCP listener closes a connection idle beyond idle_timeout_ms
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'tcp',
          'addr' = '{{syslog_ingest_addr}}',
          'idle_timeout_ms' = '1000'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When a Syslog client that sends nothing connects to "{{syslog_ingest_addr}}"
    And Syslog TCP messages are published with mixed framing to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}Oct 11 22:14:15 edge-2 worker: beside an idle connection
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-2 worker 456 ID48 - after it
      """
    Then the relay subscription receives a payload
      """
      "message":"beside an idle connection"
      """
    And the relay subscription receives a payload
      """
      "message":"after it"
      """
    And the Syslog client that sends nothing is disconnected no sooner than "1s" after it connected

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A Syslog UDP datagram larger than max_message_size is dropped and counted
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_ingest_addr}}',
          'max_message_size' = '512'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "10s" node "node-1" eventually reports describe ingestor "syslog_intake" as "status: running"
    When a Syslog UDP datagram of 1024 bytes is published to "{{syslog_ingest_addr}}"
    And Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 - after an oversize datagram
      """
    Then the relay subscription receives a payload
      """
      "message":"after an oversize datagram"
      """
    And within "10s" node "node-1" eventually reports describe ingestor "syslog_intake" as "nervix_ingestor_oversized_payloads_total: 1"
    And node "node-1" observability metric "nervix_ingestor_oversized_payloads_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="syslog_intake"
      physical_node_id="node-1"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # The harness draws the ingest port when the scenario starts, and every node binds it only once
  # the listener starts after START. A socket that took the port in between fails every bind the
  # listener retries, so the address moves to a fresh port when a statement first names it.
  @node_bound_port_taken
  Scenario Outline: A Syslog UDP listener binds a fresh port when another socket took the port the scenario drew for it
    Given a <cluster_size> node nervix cluster is started
    And another socket binds UDP on the port the scenario drew for its syslog ingest address
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_event (
        facility U8,
        severity U8,
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY syslog_events SCHEMA syslog_event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_listener
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_ingest_addr}}'
        };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      ready: true
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 - moved listener
      """
    Then the relay subscription receives a payload
      """
      "message":"moved listener"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

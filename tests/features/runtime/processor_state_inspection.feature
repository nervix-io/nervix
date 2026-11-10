Feature: Bounded typed reorderer and correlator state inspection
  @processor_state_inspection
  Scenario Outline: A reorderer lists the rows it holds with their order keys and release deadline until it releases them
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( region STRING, sequence I64, secret STRING SENSITIVE );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( region string, sequence integer, secret string );
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE SCHEMA region_key ( region STRING );
      CREATE BRANCH by_region SCHEMA region_key TTL 5m;
      CREATE RELAY incoming SCHEMA notification BRANCHED BY by_region;
      CREATE RELAY ordered SCHEMA notification BRANCHED BY by_region;
      CREATE VHOST edge reorderer-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/notifications';
      CREATE HTTP ENDPOINT INGESTOR notifications FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO incoming INHERIT ALL BRANCHED BY by_region SET region = message.region
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REORDERER held_notifications
        FROM incoming
        BY input.sequence
        MAX TIME 10m
        BRANCHED BY by_region
        TO ordered INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION ordered_evidence TO ordered;
      START;
      """
    And http payload is posted to node "node-1" with host "reorderer-state-{{test_id}}.example.com" path "/notifications"
      """
      {"region":"east","sequence":3,"secret":"east-three"}
      """
    And http payload is posted to node "node-1" with host "reorderer-state-{{test_id}}.example.com" path "/notifications"
      """
      {"region":"west","sequence":2,"secret":"west-two"}
      """
    And http payload is posted to node "node-1" with host "reorderer-state-{{test_id}}.example.com" path "/notifications"
      """
      {"region":"east","sequence":1,"secret":"east-one"}
      """
    And http payload is posted to node "node-1" with host "reorderer-state-{{test_id}}.example.com" path "/notifications"
      """
      {"region":"west","sequence":1,"secret":"west-one"}
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees reorderer "held_notifications" hold 4 rows
    And the last processor state counts 4 rows in 2 branches
    And the last processor state section "pending" rows match
      """
      {"branch":{"region":"east"},"fields":{"region":"east","sequence":3,"secret":"\u003cmasked\u003e","output":0,"order_key_1":3}}
      {"branch":{"region":"east"},"fields":{"region":"east","sequence":1,"secret":"\u003cmasked\u003e","output":0,"order_key_1":1}}
      {"branch":{"region":"west"},"fields":{"region":"west","sequence":2,"secret":"\u003cmasked\u003e","output":0,"order_key_1":2}}
      {"branch":{"region":"west"},"fields":{"region":"west","sequence":1,"secret":"\u003cmasked\u003e","output":0,"order_key_1":1}}
      """
    And the last processor state releases every "pending" row "10m" after the earliest held row of its branch was received
    And the last processor state never carries the bytes of "east-three", "east-one", "west-two" or "west-one"
    When inspection client "reader" connects away from the last relay owner when possible
    And client "reader" executes these Plexus commands
      """
      SHOW REORDERER held_notifications STATE FOR BRANCH VALUES { region = 'west' } LIMIT 10 FORMAT JSON;
      """
    Then the last processor state counts 2 rows in 1 branches
    And the typed inspection used its unit owner and a remote page when the cluster permits it
    And the last processor state section "pending" rows match
      """
      {"branch":{"region":"west"},"fields":{"region":"west","sequence":2,"secret":"\u003cmasked\u003e","output":0,"order_key_1":2}}
      {"branch":{"region":"west"},"fields":{"region":"west","sequence":1,"secret":"\u003cmasked\u003e","output":0,"order_key_1":1}}
      """
    And the last inspection JSON is the complete typed envelope
    And client "reader" reads reorderer "held_notifications" state one row per page with unchanged complete counts
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE REORDERER held_notifications;
      """
    Then the last command output contains
      """
      branches: 2
      rows: 4
      state bytes:
      """
    When client "reader" saves a one row page of reorderer "held_notifications" state
    And http payload is posted to node "node-1" with host "reorderer-state-{{test_id}}.example.com" path "/notifications"
      """
      {"region":"east","sequence":2,"secret":"east-two"}
      """
    Then within "30s" client "reader" sees reorderer "held_notifications" hold 5 rows
    When client "reader" resumes the saved reorderer "held_notifications" state page
    Then the typed inspection refuses the read with "stale_continuation"
    When client "reader" submits this Plexus command request
      """
      SHOW REORDERER absent STATE FORMAT JSON;
      """
    Then the typed inspection refuses the read with "unknown_entity"
    When client "reader" submits this Plexus command request
      """
      SHOW REORDERER held_notifications STATE FOR BRANCH VALUES { absent = 'value' } FORMAT JSON;
      """
    Then the typed inspection refuses the read with "invalid_selection"
    # Shortening MAX TIME is dynamic: each branch releases what it holds, in key order, on its next tick.
    When these Plexus commands are executed on the leader node
      """
      ALTER REORDERER held_notifications SET MAX TIME 1ms;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "region":"east" | "sequence":1
      "region":"east" | "sequence":2
      "region":"east" | "sequence":3
      "region":"west" | "sequence":1
      "region":"west" | "sequence":2
      """
    And within "30s" client "reader" sees reorderer "held_notifications" hold 0 rows
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE REORDERER held_notifications;
      """
    Then the last command output contains
      """
      rows: 0
      state bytes: 0
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @processor_state_inspection
  Scenario Outline: A correlator lists unmatched records under their side with the received time and timeout deadline until a match arrives
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA order_event ( tenant STRING SENSITIVE, order_id I64, note STRING SENSITIVE );
      CREATE SCHEMA payment_event ( tenant STRING SENSITIVE, order_id I64, amount I64 );
      CREATE SCHEMA paid_order ( tenant STRING SENSITIVE, order_id I64, amount I64 );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT ( tenant string, order_id integer, note string );
      CREATE WIRE JSON SCHEMA payment_wire MODE STRICT ( tenant string, order_id integer, amount integer );
      CREATE WIRE JSON CODEC order_codec FROM WIRE SCHEMA order_wire TO SCHEMA order_event;
      CREATE WIRE JSON CODEC payment_codec FROM WIRE SCHEMA payment_wire TO SCHEMA payment_event;
      CREATE SCHEMA tenant_key ( tenant STRING SENSITIVE );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY orders SCHEMA order_event BRANCHED BY by_tenant;
      CREATE RELAY payments SCHEMA payment_event BRANCHED BY by_tenant;
      CREATE RELAY paid SCHEMA paid_order BRANCHED BY by_tenant;
      CREATE VHOST edge correlator-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT order_ingress ON edge PATH '/orders';
      CREATE HTTP ENDPOINT payment_ingress ON edge PATH '/payments';
      CREATE HTTP ENDPOINT INGESTOR order_ingestor FROM order_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING order_codec
        TO orders INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR payment_ingestor FROM payment_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING payment_codec
        TO payments INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CORRELATOR settle_orders
        LEFT FROM orders
        RIGHT FROM payments
        CORRELATE WHERE left.order_id = right.order_id
        MATCH EARLIEST
        MAX TIME 10m
        ON CORRELATION TIMEOUT DROP, DROP
        BRANCHED BY by_tenant
        TO paid
          SET tenant = left.tenant,
            order_id = left.order_id,
            amount = right.amount
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION paid_evidence TO paid;
      START;
      """
    And http payload is posted to node "node-1" with host "correlator-state-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"acme","order_id":1,"note":"acme-note"}
      """
    And http payload is posted to node "node-1" with host "correlator-state-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"globex","order_id":2,"note":"globex-note"}
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees correlator "settle_orders" hold 2 rows
    And the last processor state counts 2 rows in 2 branches
    And the last processor state section "left" rows match
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","order_id":1,"note":"\u003cmasked\u003e"}}
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","order_id":2,"note":"\u003cmasked\u003e"}}
      """
    And the last processor state has no "right" rows
    And every "left" row of the last processor state times out "10m" after it was received
    And the last processor state never carries the bytes of "acme", "globex", "acme-note" or "globex-note"
    When inspection client "reader" connects away from the last relay owner when possible
    And client "reader" executes these Plexus commands
      """
      SHOW CORRELATOR settle_orders STATE FOR BRANCH VALUES { tenant = 'globex' } FORMAT TEXT;
      """
    Then the last processor state counts 1 rows in 1 branches
    And the typed inspection used its unit owner and a remote page when the cluster permits it
    And the last processor state section "left" rows match
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","order_id":2,"note":"\u003cmasked\u003e"}}
      """
    And the last inspection TEXT uses its typed rows after the owner header
    And client "reader" reads correlator "settle_orders" state one row per page with unchanged complete counts
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CORRELATOR settle_orders;
      """
    Then the last command output contains
      """
      branches: 2
      rows: 2
      state bytes:
      """
    When http payload is posted to node "node-1" with host "correlator-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"acme","order_id":1,"amount":10}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "order_id":1 | "amount":10
      """
    And within "30s" client "reader" sees correlator "settle_orders" hold 1 rows
    And the last processor state section "left" rows match
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","order_id":2,"note":"\u003cmasked\u003e"}}
      """
    When http payload is posted to node "node-1" with host "correlator-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"globex","order_id":3,"amount":7}
      """
    Then within "30s" client "reader" sees correlator "settle_orders" hold 2 rows
    And the last processor state section "left" rows match
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","order_id":2,"note":"\u003cmasked\u003e"}}
      """
    And the last processor state section "right" rows match
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","order_id":3,"amount":7}}
      """
    And every "right" row of the last processor state times out "10m" after it was received
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @processor_state_inspection
  Scenario Outline: Inspecting a reorderer during steady input changes neither its output order nor its counts
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( region STRING, sequence I64, secret STRING SENSITIVE );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( region string, sequence integer, secret string );
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE SCHEMA region_key ( region STRING );
      CREATE BRANCH by_region SCHEMA region_key TTL 5m;
      CREATE RELAY incoming SCHEMA notification BRANCHED BY by_region;
      CREATE RELAY ordered SCHEMA notification BRANCHED BY by_region;
      CREATE VHOST edge steady-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/notifications';
      CREATE HTTP ENDPOINT INGESTOR notifications FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO incoming INHERIT ALL BRANCHED BY by_region SET region = message.region
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REORDERER steady_notifications
        FROM incoming
        BY input.sequence
        MAX TIME 300ms
        BRANCHED BY by_region
        TO ordered INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION ordered_evidence TO ordered;
      START;
      """
    Given client "reader" is connected to node "node-1"
    When client "reader" inspects reorderer "steady_notifications" after each of these payloads posted to node "node-1" with host "steady-state-{{test_id}}.example.com" path "/notifications"
      """
      {"region":"east","sequence":1,"secret":"s"}
      {"region":"west","sequence":1,"secret":"s"}
      {"region":"east","sequence":2,"secret":"s"}
      {"region":"west","sequence":2,"secret":"s"}
      {"region":"east","sequence":3,"secret":"s"}
      {"region":"west","sequence":3,"secret":"s"}
      {"region":"east","sequence":4,"secret":"s"}
      {"region":"west","sequence":4,"secret":"s"}
      {"region":"east","sequence":5,"secret":"s"}
      {"region":"west","sequence":5,"secret":"s"}
      {"region":"east","sequence":6,"secret":"s"}
      {"region":"west","sequence":6,"secret":"s"}
      """
    Then within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "region":"east" | "sequence":1
      "region":"east" | "sequence":2
      "region":"east" | "sequence":3
      "region":"east" | "sequence":4
      "region":"east" | "sequence":5
      "region":"east" | "sequence":6
      "region":"west" | "sequence":1
      "region":"west" | "sequence":2
      "region":"west" | "sequence":3
      "region":"west" | "sequence":4
      "region":"west" | "sequence":5
      "region":"west" | "sequence":6
      """
    And the relay subscription does not receive a payload within "1s"
    And within "30s" client "reader" sees reorderer "steady_notifications" hold 0 rows
    And the last processor state counts 0 rows in 2 branches

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @processor_state_inspection
  Scenario Outline: A stopped reorderer or correlator reports every state count as unavailable because no unit runs
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( region STRING, sequence I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( region string, sequence integer );
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE SCHEMA region_key ( region STRING );
      CREATE BRANCH by_region SCHEMA region_key TTL 5m;
      CREATE RELAY incoming SCHEMA notification BRANCHED BY by_region;
      CREATE RELAY settlements SCHEMA notification BRANCHED BY by_region;
      CREATE RELAY ordered SCHEMA notification BRANCHED BY by_region;
      CREATE RELAY settled SCHEMA notification BRANCHED BY by_region;
      CREATE VHOST edge stopped-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/notifications';
      CREATE HTTP ENDPOINT INGESTOR notifications FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO incoming INHERIT ALL BRANCHED BY by_region SET region = message.region
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REORDERER held_notifications
        FROM incoming
        BY input.sequence
        MAX TIME 10m
        BRANCHED BY by_region
        TO ordered INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE CORRELATOR settle_notifications
        LEFT FROM incoming
        RIGHT FROM settlements
        CORRELATE WHERE left.sequence = right.sequence
        MATCH EARLIEST
        MAX TIME 10m
        ON CORRELATION TIMEOUT DROP, DROP
        BRANCHED BY by_region
        TO settled
          SET region = left.region,
            sequence = left.sequence
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "stopped-state-{{test_id}}.example.com" path "/notifications"
      """
      {"region":"east","sequence":1}
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees reorderer "held_notifications" hold 1 rows
    And within "30s" client "reader" sees correlator "settle_notifications" hold 1 rows
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE REORDERER held_notifications;
      """
    Then the last command output contains
      """
      branches: 1
      rows: 1
      """
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    And these Plexus commands are executed on the leader node
      """
      DESCRIBE REORDERER held_notifications;
      """
    Then the last command output contains
      """
      replicated state: true
      outputs: 1
      output 0: into=ordered construction=present branch=NODE-WIDE flush=1h max-batch-size=1MiB
      labels: {}
      branches: unavailable (owner_unavailable)
      rows: unavailable (owner_unavailable)
      state bytes: unavailable (owner_unavailable)
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CORRELATOR settle_notifications;
      """
    Then the last command output contains
      """
      replicated state: true
      outputs: 1
      output 0: into=settled construction=present branch=NODE-WIDE flush=IMMEDIATE
      labels: {}
      branches: unavailable (owner_unavailable)
      rows: unavailable (owner_unavailable)
      state bytes: unavailable (owner_unavailable)
      """
    When client "reader" submits this Plexus command request
      """
      SHOW REORDERER held_notifications STATE FORMAT JSON;
      """
    Then the typed inspection refuses the read with "owner_unavailable"
    When client "reader" submits this Plexus command request
      """
      SHOW CORRELATOR settle_notifications STATE FORMAT JSON;
      """
    Then the typed inspection refuses the read with "owner_unavailable"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

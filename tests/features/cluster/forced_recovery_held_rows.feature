@forced_recovery_held_rows
Feature: Rows a route holds on a live node survive a forced recovery elsewhere in the domain
  A route's flush is an upper bound on how long it holds rows, not a promise to hold them that
  long. When a node stops answering, failover moves what it owned to a surviving node, and every
  node that applies that change flushes what its routes hold, so a held row can leave before its
  route's cadence. Leaving early loses nothing: once a row's other copies have been delivered, the
  copy a route held on a node that stays live is delivered exactly once, in its own branch, and a
  source waiting for the row's acknowledgement hears that it completed.

  Scenario Outline: A junction's held rows survive a forced recovery of its other output relay when <failure>
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When leadership is transferred to node "node-1"
    # The junction, its input relay and its delayed route's relay run on node-1, which stays live.
    # Its other output relay runs on node-3, and node-1 is cordoned, so the recovery that moves that
    # relay off node-3 places it on node-2: node-1 only applies a change to a unit it does not run.
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA routed_event (
        id STRING,
        tenant STRING,
        route STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA routed_event_wire MODE STRICT (
        id string,
        tenant string,
        route string OPTIONAL
      );
      CREATE WIRE JSON CODEC routed_event_codec
        FROM WIRE SCHEMA routed_event_wire
        TO SCHEMA routed_event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming_events SCHEMA routed_event BRANCHED BY by_tenant;
      CREATE RELAY immediate_events SCHEMA routed_event BRANCHED BY by_tenant;
      CREATE RELAY delayed_events SCHEMA routed_event BRANCHED BY by_tenant;
      CREATE VHOST edge held-route-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT unacknowledged_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT acknowledged_ingress ON edge PATH '/acknowledged-events';
      CREATE HTTP ENDPOINT INGESTOR unacknowledged_ingestor
        FROM unacknowledged_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING routed_event_codec
        TO incoming_events
        INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR acknowledged_ingestor
        FROM acknowledged_ingress MODE ACK MAX 8 ACK TIMEOUT 5m
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING routed_event_codec
        TO incoming_events
        INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events
        FROM incoming_events
        BRANCHED BY by_tenant
        TO immediate_events
          INHERIT ALL
          SET route = "immediate"
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO delayed_events
          INHERIT ALL
          SET route = "delayed"
          FLUSH EACH 40s MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION delayed_events_subscription TO delayed_events;
      START;
      RELOCATE RELAY incoming_events ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE JUNCTION route_events ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY delayed_events ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY immediate_events ONTO NODE node-3 IGNORE PREFERENCES;
      CORDON NODE node-1;
      """
    Then node "node-1" eventually reports status containing "kind=relay name=immediate_events owner=node-3"
    And node "node-1" eventually reports status containing "kind=junction name=route_events owner=node-1"
    And node "node-1" eventually reports status containing "kind=relay name=delayed_events owner=node-1"
    Given client "immediate_reader" is connected to node "node-1" with cluster seeds
    When client "immediate_reader" executes these Plexus commands
      """
      CREATE SUBSCRIPTION immediate_events_subscription TO immediate_events;
      """
    # The two sources send rows of the two branches in turn. Each row's immediate copy arrives
    # before the next row is sent, so when the recovery begins the junction holds the delayed copy
    # of every row, and each acknowledged post still waits for its answer.
    And http payload is posted to node "node-1" with host "held-route-{{test_id}}.example.com" path "/events"
      """
      {"id":"u-1","tenant":"acme"}
      """
    Then within "30s" client "immediate_reader" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"id":"u-1","route":"immediate","tenant":"acme"}
      """
    When endpoint request "a-1" is posted on node "node-1" host "held-route-{{test_id}}.example.com" path "/acknowledged-events"
      """
      {"id":"a-1","tenant":"beta"}
      """
    Then within "30s" client "immediate_reader" receives a subscription payload
      """
      key={"tenant":"beta"} payload={"id":"a-1","route":"immediate","tenant":"beta"}
      """
    When http payload is posted to node "node-1" with host "held-route-{{test_id}}.example.com" path "/events"
      """
      {"id":"u-2","tenant":"beta"}
      """
    Then within "30s" client "immediate_reader" receives a subscription payload
      """
      key={"tenant":"beta"} payload={"id":"u-2","route":"immediate","tenant":"beta"}
      """
    When endpoint request "a-2" is posted on node "node-1" host "held-route-{{test_id}}.example.com" path "/acknowledged-events"
      """
      {"id":"a-2","tenant":"acme"}
      """
    Then within "30s" client "immediate_reader" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"id":"a-2","route":"immediate","tenant":"acme"}
      """
    When <fault>
    Then within "30s" node "node-1" eventually reports status containing "kind=relay name=immediate_events owner=node-2"
    # The flush bounds the hold from above only. The rows may leave when node-1 applies the
    # recovery, and leave no later than their 40 s cadence; the wait leaves a loaded runner room.
    And within "90s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} payload={"id":"u-1","route":"delayed","tenant":"acme"}
      key={"tenant":"beta"} payload={"id":"a-1","route":"delayed","tenant":"beta"}
      key={"tenant":"beta"} payload={"id":"u-2","route":"delayed","tenant":"beta"}
      key={"tenant":"acme"} payload={"id":"a-2","route":"delayed","tenant":"acme"}
      """
    And endpoint request "a-1" answers HTTP 200 with "completed" cause "none"
    And endpoint request "a-2" answers HTTP 200 with "completed" cause "none"
    # A row the recovery flushed early is not sent again when its cadence comes round.
    And the relay subscription does not receive a payload within "45s"

    Examples:
      | failure                          | fault                                                              |
      | its owner stops answering node-1 | application health probes from node "node-1" to node "node-3" fail |
      | its owner stops                  | node "node-3" is stopped                                           |

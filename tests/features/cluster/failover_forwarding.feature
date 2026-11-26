@failover_forwarding
Feature: A relay owner fails over while another node forwards to it
  A node hands what its ingestors accept to the node that owns the relay. When that owner stops,
  failover moves the relay to a surviving node, and every node installs the new route on its own.
  A batch a node was still forwarding to the stopped owner never reached that owner, so it goes to
  the relay's new owner instead, exactly once, whether the node installs the new route before it
  learns that the owner left the cluster or after.

  Scenario: A post forwarded to a stopped relay owner reaches the owner failover installs
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    And a node other than placeholders "leader" and "owner" is saved as placeholder "forwarder"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wire_record (
        tenant STRING,
        sequence I64
      );
      CREATE WIRE JSON SCHEMA wire_record_json MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE WIRE JSON CODEC wire_record_codec
        FROM WIRE SCHEMA wire_record_json
        TO SCHEMA wire_record;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY wire_records SCHEMA wire_record BRANCHED BY by_tenant;
      CREATE VHOST edge failover-forward-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT wire_ingress ON edge PATH '/records';
      CREATE HTTP ENDPOINT INGESTOR wire_ingestor
        FROM wire_ingress MODE ACK MAX 8 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING wire_record_codec
        TO wire_records INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE RELAY wire_records ONTO NODE {{owner}} IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=relay name=wire_records owner={{owner}}
      """
    Given client "subscriber" is connected to node "{{leader}}" with cluster seeds
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And one http payload is posted with a completed graph acknowledgement to node "{{forwarder}}" host "failover-forward-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"sequence":1,"tenant":"acme"}
      """
    When runtime installation on node "{{forwarder}}" is paused
    And node "{{owner}}" is stopped
    Then within "60s" node "{{leader}}" reports no scheduled work on "{{owner}}"
    And node "{{forwarder}}" reaches its runtime installation pause
    Given relay payloads for domain "{{domain}}" are held before their grant requests
    When one http payload begins posting in the background with a completed graph acknowledgement to node "{{forwarder}}" host "failover-forward-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"beta","sequence":2}
      """
    Then a relay payload for domain "{{domain}}" is waiting before its grant request
    When runtime installation on node "{{forwarder}}" is released
    And relay payload grant requests for domain "{{domain}}" are released
    Then the background http publish succeeds
    And within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"beta"} payload={"sequence":2,"tenant":"beta"}
      """

  Scenario: A post that finds its stopped relay owner gone before the failover route waits for that route
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    And a node other than placeholders "leader" and "owner" is saved as placeholder "forwarder"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wire_record (
        tenant STRING,
        sequence I64
      );
      CREATE WIRE JSON SCHEMA wire_record_json MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE WIRE JSON CODEC wire_record_codec
        FROM WIRE SCHEMA wire_record_json
        TO SCHEMA wire_record;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY wire_records SCHEMA wire_record BRANCHED BY by_tenant;
      CREATE VHOST edge failover-forward-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT wire_ingress ON edge PATH '/records';
      CREATE HTTP ENDPOINT INGESTOR wire_ingestor
        FROM wire_ingress MODE ACK MAX 8 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING wire_record_codec
        TO wire_records INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE RELAY wire_records ONTO NODE {{owner}} IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=relay name=wire_records owner={{owner}}
      """
    Given client "subscriber" is connected to node "{{leader}}" with cluster seeds
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And one http payload is posted with a completed graph acknowledgement to node "{{forwarder}}" host "failover-forward-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"sequence":1,"tenant":"acme"}
      """
    When runtime installation on node "{{forwarder}}" is paused
    And node "{{owner}}" is stopped
    Then within "60s" node "{{leader}}" reports no scheduled work on "{{owner}}"
    And node "{{forwarder}}" reaches its runtime installation pause
    Given relay payloads for domain "{{domain}}" are held before their grant requests
    When one http payload begins posting in the background with a completed graph acknowledgement to node "{{forwarder}}" host "failover-forward-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"beta","sequence":2}
      """
    Then a relay payload for domain "{{domain}}" is waiting before its grant request
    And node "{{forwarder}}" stops counting node "{{owner}}" as a live peer
    # The forward meets the stopped owner gone from membership before this node installs the route
    # failover publishes: nothing of it reached that owner, so it waits for that route.
    When relay payload grant requests for domain "{{domain}}" are released
    And runtime installation on node "{{forwarder}}" is released
    Then the background http publish succeeds
    And within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"beta"} payload={"sequence":2,"tenant":"beta"}
      """

  Scenario: An alteration draining the forwarding ingestor when the relay owner stops leaves the post to the failover route
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    And a node other than placeholders "leader" and "owner" is saved as placeholder "forwarder"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wire_record (
        tenant STRING,
        sequence I64
      );
      CREATE WIRE JSON SCHEMA wire_record_json MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE WIRE JSON CODEC wire_record_codec
        FROM WIRE SCHEMA wire_record_json
        TO SCHEMA wire_record;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY wire_records SCHEMA wire_record BRANCHED BY by_tenant;
      CREATE VHOST edge failover-forward-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT wire_ingress ON edge PATH '/records';
      CREATE HTTP ENDPOINT INGESTOR wire_ingestor
        FROM wire_ingress MODE ACK MAX 8 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING wire_record_codec
        TO wire_records INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE RELAY wire_records ONTO NODE {{owner}} IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=relay name=wire_records owner={{owner}}
      """
    Given client "subscriber" is connected to node "{{leader}}" with cluster seeds
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And one http payload is posted with a completed graph acknowledgement to node "{{forwarder}}" host "failover-forward-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"sequence":1,"tenant":"acme"}
      """
    # Failover acts only once the alteration's hold below has ended. When the stopping owner's gate
    # cannot report its own end, the gate deadline ends that hold, so the scenario keeps the
    # deadline well inside the background post's own 30 s client timeout.
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET ENTITY_GATE_DEADLINE = 5s;
      """
    Given relay payloads for domain "{{domain}}" are held before their grant requests
    When one http payload begins posting in the background with a completed graph acknowledgement to node "{{forwarder}}" host "failover-forward-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"beta","sequence":2}
      """
    Then a relay payload for domain "{{domain}}" is waiting before its grant request
    # The alteration replaces the ingestor. Before it commits anything, it holds the ingestor's
    # intake on every node and drains the work the ingestor already took in, the post's delivery
    # among it, while no installation runs on the forwarder.
    When these Plexus commands begin executing in the background
      """
      ALTER HTTP ENDPOINT INGESTOR wire_ingestor SET MODE ACK MAX 16 ACK TIMEOUT 30s;
      """
    And node "{{owner}}" is stopped
    Then node "{{forwarder}}" stops counting node "{{owner}}" as a live peer
    # The alteration's hold ends with the owner's stop or at its gate deadline, and the alteration
    # fails. The delivery finds the owner gone and waits for the route that failover then
    # publishes, which nothing on the forwarder holds back.
    When relay payload grant requests for domain "{{domain}}" are released
    Then within "60s" node "{{leader}}" reports no scheduled work on "{{owner}}"
    And the background http publish succeeds
    And within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"beta"} payload={"sequence":2,"tenant":"beta"}
      """
    And within "60s" the background Plexus execution reaches a terminal outcome

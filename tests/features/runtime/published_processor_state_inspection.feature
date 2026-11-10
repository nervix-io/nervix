Feature: Typed inspection of published deduplicator keys and window state
  @published_processor_state_inspection
  Scenario Outline: A deduplicator exposes typed redacted keys per branch with bounded pages and complete counts
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, event_id I64, secret STRING SENSITIVE);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, event_id integer, secret string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge published-dedup-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR events FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR remembered FROM incoming
        DEDUPLICATE ON input.event_id, input.secret MAX TIME 1h BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION unique_evidence TO unique_events;
      START;
      """
    And http payload is posted to node "node-1" with host "published-dedup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"east","event_id":1,"secret":"east-one"}
      """
    And http payload is posted to node "node-1" with host "published-dedup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"west","event_id":1,"secret":"west-one"}
      """
    And http payload is posted to node "node-1" with host "published-dedup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"east","event_id":2,"secret":"east-two"}
      """
    And http payload is posted to node "node-1" with host "published-dedup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"west","event_id":2,"secret":"west-two"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"east"} | "event_id":1
      key={"tenant":"west"} | "event_id":1
      key={"tenant":"east"} | "event_id":2
      key={"tenant":"west"} | "event_id":2
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees deduplicator "remembered" hold 4 rows
    Then the last processor state counts 4 rows in 2 branches
    And the last processor state section "keys" rows match
      """
      {"branch":{"tenant":"east"},"fields":{"key_1":1,"key_2":"\u003cmasked\u003e"}}
      {"branch":{"tenant":"east"},"fields":{"key_1":2,"key_2":"\u003cmasked\u003e"}}
      {"branch":{"tenant":"west"},"fields":{"key_1":1,"key_2":"\u003cmasked\u003e"}}
      {"branch":{"tenant":"west"},"fields":{"key_1":2,"key_2":"\u003cmasked\u003e"}}
      """
    And the last deduplicator state dates every visible key
    And the last processor state redacts "key_2" in section "keys"
    And the last processor state never carries the bytes of "east-one", "east-two", "west-one" or "west-two"
    And the last inspection JSON is the complete typed envelope
    And client "reader" reads deduplicator "remembered" state one row per page with unchanged complete counts
    When inspection client "reader" connects away from the last relay owner when possible
    And client "reader" executes these Plexus commands
      """
      SHOW DEDUPLICATOR remembered STATE FOR BRANCH KEY { tenant = 'west' } LIMIT 1 FORMAT JSON;
      """
    Then the last processor state counts 2 rows in 1 branches
    And the typed inspection used its unit owner and a remote page when the cluster permits it
    When client "reader" executes these Plexus commands
      """
      SHOW DEDUPLICATOR remembered STATE FORMAT JSON;
      """
    And these Plexus commands are executed on the leader node
      """
      DESCRIBE DEDUPLICATOR remembered;
      """
    Then the last command output contains
      """
      branches: 2
      keys: 4
      state bytes:
      """
    And the last processor description matches the last inspected counts

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @published_processor_state_inspection
  Scenario Outline: A window exposes retained columns and branch metadata across a step
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, value I64, secret STRING SENSITIVE);
      CREATE SCHEMA summary (samples I64, total I64, tag STRING SENSITIVE);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value integer, secret string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY summaries SCHEMA summary BRANCHED BY by_tenant;
      CREATE VHOST edge published-window-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR events FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR retained FROM incoming
        WIDTH 3 MESSAGES 1h DURATION STEP 3 MESSAGES MAX STATE SIZE 1MiB BRANCHED BY by_tenant
        TO summaries SET samples = COUNT(input.value), total = SUM(input.value), tag = FIRST(input.secret) ON MESSAGE ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "published-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"east","value":10,"secret":"east-ten"}
      """
    And http payload is posted to node "node-1" with host "published-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"west","value":100,"secret":"west-hundred"}
      """
    And http payload is posted to node "node-1" with host "published-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"east","value":20,"secret":"east-twenty"}
      """
    And http payload is posted to node "node-1" with host "published-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"west","value":200,"secret":"west-two-hundred"}
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees window "retained" hold 4 rows
    Then the last processor state counts 4 rows in 2 branches
    And the last processor state section "rows" rows match
      """
      {"branch":{"tenant":"east"},"fields":{"tenant":"east","value":10,"secret":"\u003cmasked\u003e","sequence":0,"argument_0":10,"argument_1":10,"argument_2":"\u003cmasked\u003e"}}
      {"branch":{"tenant":"east"},"fields":{"tenant":"east","value":20,"secret":"\u003cmasked\u003e","sequence":1,"argument_0":20,"argument_1":20,"argument_2":"\u003cmasked\u003e"}}
      {"branch":{"tenant":"west"},"fields":{"tenant":"west","value":100,"secret":"\u003cmasked\u003e","sequence":0,"argument_0":100,"argument_1":100,"argument_2":"\u003cmasked\u003e"}}
      {"branch":{"tenant":"west"},"fields":{"tenant":"west","value":200,"secret":"\u003cmasked\u003e","sequence":1,"argument_0":200,"argument_1":200,"argument_2":"\u003cmasked\u003e"}}
      """
    And the last window state has 2 branch headers for 2 retained rows each
    And the last processor state redacts "secret" in section "rows"
    And the last processor state redacts "argument_2" in section "rows"
    And the last processor state never carries the bytes of "east-ten", "east-twenty", "west-hundred" or "west-two-hundred"
    And the last inspection JSON is the complete typed envelope
    And client "reader" reads window "retained" state one row per page with unchanged complete counts
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE WINDOW PROCESSOR retained;
      """
    Then the last command output contains
      """
      branches: 2
      rows: 4
      state bytes:
      """
    And the last processor description matches the last inspected counts

    When http payload is posted to node "node-1" with host "published-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"east","value":30,"secret":"east-thirty"}
      """
    Then within "30s" client "reader" sees window "retained" hold 2 rows
    And the last processor state counts 2 rows in 2 branches
    And the last processor state section "rows" rows match
      """
      {"branch":{"tenant":"west"},"fields":{"tenant":"west","value":100,"secret":"\u003cmasked\u003e","sequence":0,"argument_0":100,"argument_1":100,"argument_2":"\u003cmasked\u003e"}}
      {"branch":{"tenant":"west"},"fields":{"tenant":"west","value":200,"secret":"\u003cmasked\u003e","sequence":1,"argument_0":200,"argument_1":200,"argument_2":"\u003cmasked\u003e"}}
      """
    When inspection client "reader" connects away from the last relay owner when possible
    And client "reader" executes these Plexus commands
      """
      SHOW WINDOW PROCESSOR retained STATE FOR BRANCH KEY { tenant = 'west' } LIMIT 1 FORMAT JSON;
      """
    Then the last processor state counts 2 rows in 1 branches
    And the typed inspection used its unit owner and a remote page when the cluster permits it

    When client "reader" executes these Plexus commands
      """
      SHOW WINDOW PROCESSOR retained STATE FOR BRANCH KEY { tenant = 'east' } LIMIT 1 FORMAT JSON;
      """
    Then the last processor state counts 0 rows in 1 branches
    And the last processor state has no "rows" rows
    And the last window state keeps an empty branch header after sequence 3
    And the last inspection JSON is the complete typed envelope

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @published_processor_state_inspection
  Scenario Outline: A published deduplicator key expires from inspection without another input
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, event_id I64, secret STRING SENSITIVE);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, event_id integer, secret string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge published-dedup-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR events FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR remembered FROM incoming
        DEDUPLICATE ON input.event_id, input.secret MAX TIME 5s BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION unique_evidence TO unique_events;
      START;
      """
    When http payload is posted to node "node-1" with host "published-dedup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"east","event_id":1,"secret":"expiry-evidence"}
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees deduplicator "remembered" hold 1 rows
    And the last deduplicator state dates every visible key
    And within "30s" client "reader" sees deduplicator "remembered" hold 0 rows
    And the last processor state has no "keys" rows
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DEDUPLICATOR remembered;
      """
    Then the last command output contains
      """
      keys: 0
      state bytes: 0
      """
    And the last processor description matches the last inspected counts

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

Feature: Bounded typed materialized relay inspection
  @materialized_state_inspection
  Scenario Outline: An empty materialized relay has a typed empty JSON inspection
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      START;
      """
    And these Plexus commands are executed through the client on node "node-1"
      """
      SHOW RELAY events MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /entity/name = "events"
      /counts/rows = 0
      """
    And the last typed state inspection is empty
    When the CLI successfully executes this JSON inspection
      """
      SHOW RELAY events MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /entity/name = "events"
      /counts/rows = 0
      /outcome = "page"
      """
    When the CLI refuses this JSON inspection
      """
      SHOW RELAY absent MATERIALIZED STATE FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /entity/name = "absent"
      /outcome = "rejected"
      /rejection = "unknown_entity"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @materialized_state_inspection
  Scenario Outline: Typed owner pages keep exact branch state, sensitivity and continuation identity
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING SENSITIVE, secret STRING SENSITIVE, seq I64, ingested_at_low_watermark I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, secret string, seq integer, ingested_at_low_watermark integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING SENSITIVE );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY plain SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY root_events SCHEMA event UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge typed-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT root_ingress ON edge PATH '/root';
      CREATE HTTP ENDPOINT INGESTOR root_ingestor FROM root_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO root_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION evidence TO events;
      START;
      """
    And http payload is posted to node "node-1" with host "typed-state-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha-private","secret":"alpha-first","seq":1,"ingested_at_low_watermark":7}
      """
    And http payload is posted to node "node-1" with host "typed-state-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta-private","secret":"beta-first","seq":2,"ingested_at_low_watermark":8}
      """
    And http payload is posted to node "node-1" with host "typed-state-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha-private","secret":"alpha-final","seq":3,"ingested_at_low_watermark":7}
      """
    And http payload is posted to node "node-1" with host "typed-state-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta-private","secret":"beta-final","seq":4,"ingested_at_low_watermark":8}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1
      "seq":2
      "seq":3
      "seq":4
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "events" containing
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","secret":"\u003cmasked\u003e","seq":3,"ingested_at_low_watermark":7}}
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","secret":"\u003cmasked\u003e","seq":4,"ingested_at_low_watermark":8}}
      """
    Given client "reader" is connected to node "node-1"
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 2 FORMAT JSON;
      """
    Then the last typed inspection has 2 returned rows and 2 complete rows
    And the last inspection JSON is the complete typed envelope
    And the last typed inspection rows match
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","secret":"\u003cmasked\u003e","seq":3,"ingested_at_low_watermark":7}}
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","secret":"\u003cmasked\u003e","seq":4,"ingested_at_low_watermark":8}}
      """
    And the complete inspection row order is saved
    When inspection client "reader" connects away from the last relay owner when possible
    And client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1 FORMAT JSON;
      """
    Then the last typed inspection has 1 returned rows and 2 complete rows
    And the typed inspection used its unit owner and a remote page when the cluster permits it
    And the last inspection page is the saved complete inspection's first row
    And the typed inspection preserves redaction and distinct watermark columns
    And the inspection continuation is saved with its complete counts and rows
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR BRANCH KEY { tenant = 'beta-private' } LIMIT 1 FORMAT TEXT;
      """
    Then the last typed inspection has 1 returned rows and 1 complete rows
    And the last inspection TEXT uses its typed rows after the owner header
    And the last typed inspection rows match
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","secret":"\u003cmasked\u003e","seq":4,"ingested_at_low_watermark":8}}
      """
    When inspection client "reader" resumes relay "events" with the saved continuation
    Then the last typed inspection has 1 returned rows and 2 complete rows
    And the completed inspection has exactly 2 distinct rows with unchanged complete counts
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1 FORMAT JSON;
      """
    Then the inspection continuation is saved with its complete counts and rows
    When http payload is posted to node "node-1" with host "typed-state-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha-private","secret":"alpha-new-generation","seq":5,"ingested_at_low_watermark":9}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":5
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "events" containing
      """
      {"branch":{"tenant":"\u003cmasked\u003e"},"fields":{"tenant":"\u003cmasked\u003e","secret":"\u003cmasked\u003e","seq":5,"ingested_at_low_watermark":9}}
      """
    And the materialized inspection cut differs from the saved continuation
    When inspection client "reader" resumes relay "events" with the saved continuation
    Then the typed inspection refuses the read with "stale_continuation"
    When client "reader" submits this Plexus command request
      """
      SHOW RELAY plain MATERIALIZED STATE FORMAT JSON;
      """
    Then the typed inspection refuses the read with "not_materialized"
    When client "reader" submits this Plexus command request
      """
      SHOW RELAY absent MATERIALIZED STATE FORMAT JSON;
      """
    Then the typed inspection refuses the read with "unknown_entity"
    When client "reader" submits this Plexus command request
      """
      SHOW RELAY events MATERIALIZED STATE LIMIT 18446744073709551615 FORMAT JSON;
      """
    Then the typed inspection refuses the read with "invalid_limit"
    When client "reader" submits this Plexus command request
      """
      SHOW RELAY events MATERIALIZED STATE FOR BRANCH KEY { absent = 'value' } FORMAT JSON;
      """
    Then the typed inspection refuses the read with "invalid_selection"
    When http payload is posted to node "node-1" with host "typed-state-{{test_id}}.example.com" path "/root"
      """
      {"tenant":"root-private","secret":"root-secret","seq":99,"ingested_at_low_watermark":17}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "root_events" containing
      """
      {"branch":null,"fields":{"tenant":"\u003cmasked\u003e","secret":"\u003cmasked\u003e","seq":99,"ingested_at_low_watermark":17}}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY root_events MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;
      """
    Then the last typed inspection has 1 returned rows and 1 complete rows
    And the last inspection JSON is the complete typed envelope
    And the last typed inspection rows match
      """
      {"branch":null,"fields":{"tenant":"\u003cmasked\u003e","secret":"\u003cmasked\u003e","seq":99,"ingested_at_low_watermark":17}}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @materialized_state_inspection
  Scenario: A materialized page read from another node waits for bulk memory on the reading node
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge waiting-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "waiting-state-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","seq":1}
      """
    And http payload is posted to node "node-1" with host "waiting-state-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "events" containing
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","seq":1}}
      {"branch":{"tenant":"beta"},"fields":{"tenant":"beta","seq":2}}
      """
    Given client "reader" is connected to node "node-1"
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 2 FORMAT JSON;
      """
    Then the last typed inspection has 2 returned rows and 2 complete rows
    # The reading node keeps room for transport and the page's descriptor, but not for opening its
    # container: decoding a group's identities alone takes the 1 MiB identity record bound.
    When inspection client "reader" connects away from the last relay owner when possible
    And bulk memory on the reading node keeps only "524288" bytes free
    And client "reader" begins executing these Plexus commands in the background
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 2 FORMAT JSON;
      """
    Then the background Plexus execution is still running after "2s"
    When bulk memory on the reading node is released
    Then within "30s" the background Plexus execution reaches a successful outcome
    And the background read answered 2 rows of 2 from a remote owner

  @materialized_state_inspection
  Scenario: A materialized page read from another node waits for the owner's bulk memory to hold its first row
    Given a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, value I64, message STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, message string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge first-row-relay-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingest FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 4MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload for tenant "north" with value 1 and a 1048576 byte message is posted to host "first-row-relay-{{test_id}}.example.com" path "/events"
    And http payload for tenant "south" with value 2 and a 1048576 byte message is posted to host "first-row-relay-{{test_id}}.example.com" path "/events"
    Then within "30s" a client of every node sees relay "events" hold 2 materialized rows
    Given client "reader" is connected to node "node-1"
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 2 FORMAT JSON;
      """
    Then the last typed inspection has 2 returned rows and 2 complete rows
    # The owner keeps room for the page's descriptor, but not for encoding its first row, which
    # carries a 1 MiB message. The first row is what makes the read progress, so the owner waits
    # for room for it, holding nothing, rather than refusing the read.
    When inspection client "reader" connects away from the last relay owner when possible
    And bulk memory on the owner of the last read relay keeps only "524288" bytes free
    And client "reader" begins executing these Plexus commands in the background
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 2 FORMAT JSON;
      """
    Then the background Plexus execution is still running after "2s"
    # Once the memory is released, the owner's page runs again within the share its first row
    # needed: it returns that row, and its continuation carries the other.
    When bulk memory on every node is released
    Then within "30s" the background Plexus execution reaches a successful outcome
    And the background read answered 1 row of 2 from a remote owner

  @materialized_state_inspection
  Scenario: A materialized relay read the owner's bulk memory cannot hold in one page pages through every row
    Given a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, value I64, message STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, message string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge paged-relay-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingest FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 4MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload for tenant "north" with value 1 and a 1048576 byte message is posted to host "paged-relay-{{test_id}}.example.com" path "/events"
    And http payload for tenant "south" with value 2 and a 1048576 byte message is posted to host "paged-relay-{{test_id}}.example.com" path "/events"
    And http payload for tenant "east" with value 3 and a 1048576 byte message is posted to host "paged-relay-{{test_id}}.example.com" path "/events"
    And http payload for tenant "west" with value 4 and a 1048576 byte message is posted to host "paged-relay-{{test_id}}.example.com" path "/events"
    And http payload for tenant "centre" with value 5 and a 1048576 byte message is posted to host "paged-relay-{{test_id}}.example.com" path "/events"
    And http payload for tenant "edge" with value 6 and a 1048576 byte message is posted to host "paged-relay-{{test_id}}.example.com" path "/events"
    Then within "30s" a client of every node sees relay "events" hold 6 materialized rows
    # Each record carries its 1 MiB message. A page another node reads sets aside, for every row
    # it selects, what encoding that row into the page's container adds, by the container
    # encoder's own charges. With 8 MiB left on the relay's owner, every other node's reads return
    # two rows a page, each page ending where the next row no longer fits, and the rest follow on
    # the next pages.
    When bulk memory on the owner of the last read relay keeps only "8388608" bytes free
    Then a client of every other node reads relay "events" materialized state at the default limit in more than one page with unchanged complete counts
    When bulk memory on every node is released
    Then within "30s" every node reserves only the bulk memory the scenario holds

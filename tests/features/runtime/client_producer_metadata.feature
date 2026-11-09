@client_ingestor @client_producer_metadata
Feature: Authenticated client producer metadata
  A producer carries the identity of the session that opened it to the ingestor owner. Payload
  fields cannot impersonate that identity, including when several users share one forwarding link.

  Scenario Outline: Client producer routes and arrival filters read the authenticated user on every batch row
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE USER metadata_alice WITH PASSWORD 'alice-password';
      CREATE USER metadata_bob WITH PASSWORD 'bob-password';
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64, user STRING);
      CREATE SCHEMA observed (seq I64, user STRING, who STRING OPTIONAL, path STRING);
      CREATE RELAY observations SCHEMA observed UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR all_users FROM SCHEMA event
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO observations INHERIT ALL SET who = metadata.user, path = 'all'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT PRODUCER INGESTOR alice_only FROM SCHEMA event
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        FILTER WHERE metadata.user = 'metadata_alice'
        TO observations INHERIT ALL SET who = 'metadata_alice', path = 'filtered'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observed TO observations;
      START;
      RELOCATE INGESTOR all_users ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE INGESTOR alice_only ONTO NODE node-1 IGNORE PREFERENCES;
      """
    And client "alice" is connected to node "<entry_node>" as user "METADATA_ALICE" with password "alice-password"
    And client "bob" is connected to node "<entry_node>" as user "metadata_bob" with password "bob-password"
    When within "30s" client "alice" opens producer "all alice" on ingestor "all_users" expecting fields "seq I64, user STRING"
    And within "30s" client "bob" opens producer "all bob" on ingestor "all_users" expecting fields "seq I64, user STRING"
    And within "30s" client "alice" opens producer "filtered alice" on ingestor "alice_only" expecting fields "seq I64, user STRING"
    And within "30s" client "bob" opens producer "filtered bob" on ingestor "alice_only" expecting fields "seq I64, user STRING"
    And producer "all alice" submits batch "alice rows" with rows
      | seq | user         |
      | 1   | metadata_bob |
      | 2   | metadata_bob |
    And producer "all bob" submits batch "bob rows" with rows
      | seq | user           |
      | 3   | metadata_alice |
    And producer "filtered alice" submits batch "admitted user" with rows
      | seq | user         |
      | 4   | metadata_bob |
    And producer "filtered bob" submits batch "filtered user" with rows
      | seq | user           |
      | 5   | metadata_alice |
    Then batch "alice rows" completes
    And batch "bob rows" completes
    And batch "admitted user" completes
    And batch "filtered user" completes
    When producer "all alice" submits batch "alice again" with rows
      | seq | user         |
      | 6   | metadata_bob |
    Then batch "alice again" completes
    And within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "user":"metadata_bob" | "who":"metadata_alice" | "path":"all"
      "seq":2 | "user":"metadata_bob" | "who":"metadata_alice" | "path":"all"
      "seq":3 | "user":"metadata_alice" | "who":"metadata_bob" | "path":"all"
      "seq":4 | "user":"metadata_bob" | "who":"metadata_alice" | "path":"filtered"
      "seq":6 | "user":"metadata_bob" | "who":"metadata_alice" | "path":"all"
      """
    And within "30s" the leader node describes ingestor "all_users" with
      """
      owner: node-1
      forwarded producers: <forwarded>
      """

    Examples:
      | cluster_size | entry_node | forwarded |
      | 1            | node-1     | 0         |
      | 3            | node-3     | 2         |

  Scenario Outline: A transport ingestor cannot read a client producer's user metadata
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA observed (seq I64, who STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY observations SCHEMA observed UNBRANCHED;
      CREATE VHOST edge user-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      """
    When these Plexus commands fail with "reads metadata.user in its route to 'observations', but HTTP ENDPOINT ingestors declare only"
      """
      CREATE HTTP ENDPOINT INGESTOR transport_user FROM intake
        MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO observations INHERIT ALL SET who = metadata.user
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

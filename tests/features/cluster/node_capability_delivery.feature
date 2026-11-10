Feature: Relay delivery across a node capability roll
  A retained input batch keeps its row acknowledgements while its receiver restarts with a
  different committed bound. Both concrete branches arrive once through the resulting slices.

  @node_capability_roll
  Scenario Outline: A pending two-branch batch adopts a receiver's <change> relay bound
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-2 starts with interconnect-relay-batch-bytes = <before>
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64, tenant STRING);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY records SCHEMA incoming BRANCHED BY tenants;
      CREATE CLIENT PRODUCER INGESTOR input
        FROM SCHEMA incoming MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND TIMESTAMP NOW
        TO records INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH EACH 250ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER output FROM records
        TO SCHEMA incoming MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE INGESTOR input ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER output ONTO NODE node-3 IGNORE PREFERENCES;
      """
    Then node "node-1" eventually describes node "node-2" accepting relay bodies of "<before>"
    Given client "app" is connected to node "node-3"
    When client "app" opens consumer "output" on emitter "output" expecting fields "id STRING, amount I64, tenant STRING"
    And client "app" opens producer "input" on ingestor "input" expecting fields "id STRING, amount I64, tenant STRING"
    Given node "node-3" holds its ACK view of node "node-2" at its current incarnation
    And relay payloads for domain "{{domain}}" are held before their grant requests
    When producer "input" submits 8 interleaved rows with 512-byte ids as batch "during roll"
    Then a relay payload for domain "{{domain}}" is waiting before its grant request
    And batch "during roll" remains pending for "200ms"
    When node "node-2" is stopped
    Given node-2 starts with interconnect-relay-batch-bytes = <after>
    When node "node-2" is started until public observation is available
    Then node "node-1" eventually describes node "node-2" accepting relay bodies of "<after>"
    When relay payload grant requests for domain "{{domain}}" are released
    Then consumer "output" receives 8 interleaved rows with 512-byte ids exactly once and acknowledges them
    And batch "during roll" completes
    When node "node-3" releases its held ACK view of node "node-2"
    Then relay payloads to node "node-2" in domain "{{domain}}" respect "<after>" with largest slice <rows>

    Examples:
      | change  | before | after | rows |
      | raised  | 2KiB   | 8KiB  | 4    |
      | lowered | 8KiB   | 2KiB  | 2    |

  @node_capability_unfit
  Scenario Outline: A refused oversized row leaves its channel ready for the next smaller batch with <stagger> between branch buffers
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-2 starts with interconnect-relay-batch-bytes = 2KiB
    And node-2 starts with memory-limit = 1GiB
    And node-2 starts with execution-relay-encoded-bytes = 2KiB
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64, tenant STRING);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY records SCHEMA incoming BRANCHED BY tenants;
      CREATE CLIENT PRODUCER INGESTOR input
        FROM SCHEMA incoming MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND TIMESTAMP NOW
        TO records INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH EACH 250ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER output FROM records
        TO SCHEMA incoming MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE INGESTOR input ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER output ONTO NODE node-3 IGNORE PREFERENCES;
      """
    Then node "node-1" eventually describes committed capabilities for node "node-2"
    Given relay payloads for domain "{{domain}}" are held before their grant requests
    When relay payload grant requests for domain "{{domain}}" are released
    Given client "app" is connected to node "node-3"
    When client "app" opens consumer "output" on emitter "output" expecting fields "id STRING, amount I64, tenant STRING"
    And client "app" opens producer "input" on ingestor "input" expecting fields "id STRING, amount I64, tenant STRING"
    Given ingestor "input" pauses before buffering branch '{"tenant":"globex"}'
    When producer "input" submits 2 interleaved rows with 4096-byte ids as batch "too large"
    Then ingestor "input" reaches its buffer pause for branch '{"tenant":"globex"}'
    And batch "too large" remains pending for "<stagger>"
    When ingestor "input" releases its buffer pause for branch '{"tenant":"globex"}'
    Then batch "too large" fails processing because "rejected"
    And node "node-2" has refused 2 oversized branch rows in domain "{{domain}}"
    When producer "input" submits 2 interleaved rows with 32-byte ids as batch "fits"
    Then consumer "output" receives 2 interleaved rows with 32-byte ids exactly once and acknowledges them
    And batch "fits" completes

    Examples:
      | stagger |
      | 0ms     |
      | 300ms   |

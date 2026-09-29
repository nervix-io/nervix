Feature: Client emitters
  Applications receive constructed Arrow batches from domain-owned emitters and explicitly
  acknowledge their processing before an attached producer completes.

  @client_emitter
  Scenario Outline: A client emitter keeps two source relays and two concrete branches distinct
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64, tenant STRING);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY orders SCHEMA incoming BRANCHED BY tenants;
      CREATE RELAY invoices SCHEMA incoming BRANCHED BY tenants;
      CREATE INGESTOR source_in
        FROM CLIENT SCHEMA incoming
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        TO invoices INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders, invoices
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "output" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "app" opens producer "input" on ingestor "source_in" expecting fields "id STRING, amount I64, tenant STRING"
    And producer "input" submits batch "two tenants" with rows
      | id  | amount | tenant |
      | o-1 | 1      | acme   |
      | o-2 | 2      | globex |
    And consumer "output" reads output batch "first"
    Then batch "two tenants" remains pending for "200ms"
    When output batch "first" is acknowledged
    And consumer "output" reads output batch "second"
    And output batch "second" is acknowledged
    And consumer "output" reads output batch "third"
    Then batch "two tenants" remains pending for "200ms"
    When output batch "third" is acknowledged
    And consumer "output" reads output batch "fourth"
    Then output batches "first, second, third, fourth" cover both sources and concrete branches
    And batch "two tenants" remains pending for "200ms"
    When output batch "fourth" is acknowledged
    Then batch "two tenants" completes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_emitter
  Scenario Outline: Two client emitters deliver independent copies before attached input completes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER first_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER second_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to the leader node
    When <session> opens consumer "first" on emitter "first_output" expecting fields "id STRING, cents I64"
    And <session> opens consumer "second" on emitter "second_output" expecting fields "id STRING, cents I64"
    And <session> opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "shared input" with rows
      | id  | amount |
      | o-1 | 12     |
    And consumer "first" reads output batch "first copy"
    And consumer "second" reads output batch "second copy"
    Then output batch "first copy" contains id "o-1" and cents 1200
    And output batch "second copy" contains id "o-1" and cents 1200
    And batch "shared input" remains pending for "200ms"
    When output batch "first copy" is acknowledged
    Then batch "shared input" remains pending for "200ms"
    When output batch "second copy" is acknowledged
    Then batch "shared input" completes

    Examples:
      | cluster_size | session                     |
      | 1            | client "app"                |
      | 3            | WebSocket session "browser" |

  @client_emitter
  Scenario Outline: Stopping and restarting a domain closes the consumer and creates a new endpoint
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to the leader node
    When <session> opens consumer "before stop" on emitter "app_output" expecting fields "id STRING, cents I64"
    And these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Then consumer "before stop" eventually ends
    And <session> cannot open a consumer on emitter "app_output" expecting fields "id STRING, cents I64" because "domain stopped"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    And <session> opens consumer "after start" on emitter "app_output" expecting fields "id STRING, cents I64"
    And <session> opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "after start input" with rows
      | id  | amount |
      | o-2 | 2      |
    And consumer "after start" reads output batch "after start output"
    Then output batch "after start output" contains id "o-2" and cents 200
    When output batch "after start output" is acknowledged
    Then batch "after start input" completes

    Examples:
      | cluster_size | session                     |
      | 1            | client "app"                |
      | 3            | WebSocket session "browser" |

  @client_emitter
  Scenario Outline: A client emitter declares its output schema, acknowledgement and batch contract
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id
        SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER app_output;
      """
    Then the last command output contains
      """
      TO CLIENT SCHEMA outgoing
      """
    And the last command output contains
      """
      MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER app_output;
      """
    Then the last command output contains
      """
      body: native Arrow
      """
    And the last command output contains
      """
      sink: CLIENT schema=outgoing
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER app_output SET TO CLIENT SCHEMA outgoing
        MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s;
      SHOW CREATE EMITTER app_output;
      """
    Then the last command output contains
      """
      MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
      """
    When these NSPL commands are executed on the leader node
      """
      DROP EMITTER app_output;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_emitter
  Scenario: A consumer is forwarded to the emitter owner and ends when that emitter relocates
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA incoming
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "entry"
    And a node other than placeholder "entry" is saved as placeholder "owner"
    When these NSPL commands are executed on the leader node
      """
      RELOCATE EMITTER app_output ONTO NODE {{owner}} IGNORE PREFERENCES;
      """
    Then within "30s" node "{{entry}}" eventually reports scheduled "emitter" "app_output" owner equals placeholder "owner"
    Given client "app" is connected to node "{{entry}}"
    When client "app" opens consumer "remote" on emitter "app_output" expecting fields "id STRING, cents I64"
    Then within "30s" the leader node describes emitter "app_output" with
      """
      consumers: 1
      forwarded consumers: 1
      forwarded credit: 8388608 bytes
      """
    When client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "before relocation" with rows
      | id  | amount |
      | o-3 | 9      |
    And consumer "remote" reads output batch "remote output"
    Then output batch "remote output" contains id "o-3" and cents 900
    And within "30s" the leader node describes emitter "app_output" with
      """
      retained batches: 1
      forwarded retained batches: 1
      incomplete application batches: 1
      """
    When output batch "remote output" is acknowledged
    Then batch "before relocation" completes
    And within "30s" the leader node describes emitter "app_output" with
      """
      retained batches: 0
      application ACKs: 1
      """
    When these NSPL commands are executed on the leader node
      """
      RELOCATE EMITTER app_output ONTO NODE {{entry}} IGNORE PREFERENCES;
      """
    Then consumer "remote" eventually ends

  @client_emitter
  Scenario: Client output requires explicit leakage of sensitive input
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, secret STRING SENSITIVE);
      CREATE SCHEMA outgoing (id STRING, secret STRING);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      """
    And client "app" is connected to the leader node
    When client "app" fails to execute these NSPL commands
      """
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET secret = input.secret
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      sensitive
      """
    When client "app" executes these NSPL commands
      """
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET secret = leak_sensitive(input.secret)
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

  @client_emitter
  Scenario Outline: An attached producer completes after a consumer acknowledges constructed output
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA incoming
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id
        SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to the leader node
    When <session> opens consumer "output" on emitter "app_output" expecting fields "id STRING, cents I64"
    And <session> opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "first" with rows
      | id  | amount |
      | o-1 | 12     |
    And consumer "output" reads output batch "first output"
    Then output batch "first output" contains id "o-1" and cents 1200
    And batch "first" remains pending for "200ms"
    When output batch "first output" is acknowledged
    And output batch "first output" is acknowledged
    Then batch "first" completes
    When consumer "output" closes
    And producer "input" submits batch "without consumer" with rows
      | id  | amount |
      | o-6 | 6      |
    Then batch "without consumer" remains pending for "200ms"
    When <session> opens consumer "replacement" on emitter "app_output" expecting fields "id STRING, cents I64"
    And consumer "replacement" reads output batch "recovered output"
    Then output batch "recovered output" contains id "o-6" and cents 600
    When output batch "recovered output" is acknowledged
    Then batch "without consumer" completes

    Examples:
      | cluster_size | session                     |
      | 1            | client "app"                |
      | 1            | WebSocket session "browser" |
      | 3            | client "app"                |
      | 3            | WebSocket session "browser" |

  @client_emitter
  Scenario Outline: Retrying an output batch revokes its first attempt before another consumer receives it
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA incoming
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id
        SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "first" on emitter "app_output" expecting fields "id STRING, cents I64"
    Then client "app" cannot open a consumer on emitter "app_output" expecting fields "id STRING, cents STRING" because "schema mismatch"
    When client "app" opens consumer "second" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "first input" with rows
      | id  | amount |
      | o-2 | 7      |
    And consumer "first" reads output batch "first attempt"
    And output batch "first attempt" is retried
    And consumer "second" reads output batch "second attempt"
    Then output batches "first attempt" and "second attempt" retain one identity with distinct attempts
    And output batch "first attempt" has a stale acknowledgement reference
    And batch "first input" remains pending for "200ms"
    When output batch "second attempt" is acknowledged
    Then batch "first input" completes
    When consumer "first" closes
    And consumer "second" closes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_emitter
  Scenario Outline: A detached client emitter releases its source before the application ACK
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DETACHED EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "output" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "detached input" with rows
      | id  | amount |
      | o-4 | 4      |
    Then batch "detached input" completes
    When consumer "output" reads output batch "detached output"
    Then output batch "detached output" contains id "o-4" and cents 400
    And within "30s" the leader node describes emitter "app_output" with
      """
      retained batches: 1
      """
    When output batch "detached output" is acknowledged
    Then within "30s" the leader node describes emitter "app_output" with
      """
      retained batches: 0
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_emitter
  Scenario Outline: An ACK timeout revokes a reference and redelivers unchanged output
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "output" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "timed input" with rows
      | id  | amount |
      | o-5 | 5      |
    And consumer "output" reads output batch "first attempt"
    And consumer "output" reads output batch "second attempt"
    Then output batches "first attempt" and "second attempt" retain one identity with distinct attempts
    And output batch "first attempt" has a stale acknowledgement reference
    And within "30s" the leader node describes emitter "app_output" with
      """
      retries: 1
      """
    When output batch "second attempt" is acknowledged
    Then batch "timed input" completes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_emitter
  Scenario Outline: Application rejection finishes the batch through the message error policy
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "output" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "rejected input" with rows
      | id  | amount |
      | o-7 | 7      |
    And consumer "output" reads output batch "rejected output"
    And output batch "rejected output" is rejected because "application refused"
    Then batch "rejected input" fails processing because "rejected"
    And within "30s" the leader node describes emitter "app_output" with
      """
      retained batches: 0
      application rejections: 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_emitter
  Scenario: One Arrow row larger than the client batch byte limit follows the message error policy
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1KiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "oversized input" with one 4096-byte id
    Then batch "oversized input" fails processing because "rejected"
    And within "30s" the leader node describes emitter "app_output" with
      """
      retained batches: 0
      """

  @client_emitter
  Scenario Outline: A pending output read and saturated input keep commands and domain time moving
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK PARALLEL MAX 2 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    And client "app" is connected to the leader node
    When client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64" with 2 batches and "1MiB" of credit
    And client "app" opens consumer "output" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "app" executes these NSPL commands
      """
      ATTACH DOMAIN CLOCK;
      """
    And producer "input" submits batch "b1" with rows
      | id  | amount |
      | o-8 | 8      |
    And consumer "output" reads output batch "first output"
    Then output batch "first output" contains id "o-8" and cents 800
    When consumer "output" starts waiting for output batch "second output"
    And producer "input" submits batch "b2" with rows
      | id  | amount |
      | o-9 | 9      |
    And producer "input" submits batch "b3" with rows
      | id   | amount |
      | o-10 | 10     |
    Then producer "input" holds batches "b1, b2"
    And batch "b1" remains pending for "200ms"
    And within "10s" client "app" receives a tick for its attached domain clock
    When client "app" executes these NSPL commands
      """
      SHOW CREATE EMITTER app_output;
      """
    Then the last command output contains
      """
      TO CLIENT SCHEMA outgoing
      """
    When output batch "first output" is acknowledged
    And consumer "output" finishes waiting for output batch "second output"
    Then output batch "second output" contains id "o-9" and cents 900
    When output batch "second output" is acknowledged
    And consumer "output" reads output batch "third output"
    Then output batch "third output" contains id "o-10" and cents 1000
    When output batch "third output" is acknowledged
    Then batch "b1" completes
    And batch "b2" completes
    And batch "b3" completes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_emitter
  Scenario Outline: Failed and successful emitter alterations preserve the right consumer contract
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to the leader node
    When <session> opens consumer "original" on emitter "app_output" expecting fields "id STRING, cents I64"
    And <session> opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "held input" with rows
      | id  | amount |
      | o-1 | 1      |
    And consumer "original" reads output batch "held output"
    Then batch "held input" remains pending for "200ms"
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When these NSPL commands fail with "timed out draining domain"
      """
      ALTER EMITTER app_output SET TO CLIENT SCHEMA outgoing
        MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER app_output;
      """
    Then the last command output contains
      """
      MODE ACK SEQUENTIAL ACK TIMEOUT 30s
      """
    When output batch "held output" is acknowledged
    Then batch "held input" completes
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER app_output SET FLUSH EACH 10ms MAX BATCH SIZE 1MiB;
      """
    And producer "input" submits batch "after flush input" with rows
      | id  | amount |
      | o-2 | 2      |
    And consumer "original" reads output batch "after flush output"
    Then output batch "after flush output" contains id "o-2" and cents 200
    When output batch "after flush output" is acknowledged
    Then batch "after flush input" completes
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER app_output SET TO CLIENT SCHEMA outgoing
        MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s;
      """
    Then consumer "original" eventually ends
    When <session> opens consumer "replaced" on emitter "app_output" expecting fields "id STRING, cents I64"
    And producer "input" submits batch "after replacement input" with rows
      | id  | amount |
      | o-3 | 3      |
    And consumer "replaced" reads output batch "after replacement output"
    Then output batch "after replacement output" contains id "o-3" and cents 300
    When output batch "after replacement output" is acknowledged
    Then batch "after replacement input" completes

    Examples:
      | cluster_size | session                     |
      | 1            | client "app"                |
      | 3            | WebSocket session "browser" |

  @client_emitter
  Scenario Outline: Consumer credit is reserved per session and returned on close
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And <session> is connected to the leader node
    When <session> opens consumer "one" on emitter "app_output" expecting fields "id STRING, cents I64"
    And <session> opens consumer "two" on emitter "app_output" expecting fields "id STRING, cents I64"
    And <session> opens consumer "three" on emitter "app_output" expecting fields "id STRING, cents I64"
    And <session> opens consumer "four" on emitter "app_output" expecting fields "id STRING, cents I64"
    Then <session> cannot open a consumer on emitter "app_output" expecting fields "id STRING, cents I64" because "session capacity exhausted"
    When consumer "one" closes
    And <session> opens consumer "replacement" on emitter "app_output" expecting fields "id STRING, cents I64"
    Then within "30s" the leader node describes emitter "app_output" with
      """
      consumers: 4
      """

    Examples:
      | cluster_size | session                     |
      | 1            | client "app"                |
      | 3            | WebSocket session "browser" |

  @client_emitter
  Scenario: A killed client emitter owner restarts with a fresh consumer endpoint
    Given a nervix-server process is started
    And the server process is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE INGESTOR orders_in FROM CLIENT SCHEMA incoming
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER app_output FROM orders
        TO CLIENT SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "before" is connected to the server process
    When client "before" opens consumer "before crash" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "before" opens producer "before input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "before input" submits batch "before crash input" with rows
      | id  | amount |
      | o-1 | 1      |
    And consumer "before crash" reads output batch "before crash output"
    Then output batch "before crash output" contains id "o-1" and cents 100
    When output batch "before crash output" is acknowledged
    Then batch "before crash input" completes
    When the server process receives SIGKILL
    Then the server process exits because of SIGKILL
    When the server process is restarted
    Given client "after" is connected to the server process
    When client "after" opens consumer "after crash" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "after" opens producer "after input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "after input" submits batch "after crash input" with rows
      | id  | amount |
      | o-2 | 2      |
    And consumer "after crash" reads output batch "after crash output"
    Then output batch "after crash output" contains id "o-2" and cents 200
    When output batch "after crash output" is acknowledged
    Then batch "after crash input" completes

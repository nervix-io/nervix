@client_ingestor @client_ingestor_process_faults
Feature: Client producers across real node process faults
  Every scenario runs three real nervix-server processes. The producer's session is served by one
  node, the client ingestor it publishes to executes on a second, and the ingestor's relay and its
  acknowledging HTTP emitter run on the leader, so a fault in either of the first two leaves the
  sink and the cluster's quorum running.

  Scenario: Killing an ingestor owner leaves admitted batches uncertain and restores the producer
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "hold response until released"
    And a 3 node nervix-server process cluster is started
    And the server process cluster is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (region STRING, order_id STRING, amount I64);
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And the server process cluster leader is saved as placeholder "leader"
    And a server process cluster node other than placeholder "leader" is saved as placeholder "owner"
    And a server process cluster node other than placeholders "leader" and "owner" is saved as placeholder "entry"
    And the server process cluster is configured with these NSPL commands
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{owner}} IGNORE PREFERENCES;
      RELOCATE RELAY orders ONTO NODE {{leader}} IGNORE PREFERENCES;
      RELOCATE EMITTER published ONTO NODE {{leader}} IGNORE PREFERENCES;
      """
    And client "app" is connected to server process node "{{entry}}"
    Then within "30s" the server process cluster describes ingestor "orders_in" with
      """
      owner: {{owner}}
      """
    When within "60s" client "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64"
    And producer "orders" submits batch "first" with rows
      | region | order_id | amount |
      | eu     | o-1      | 1      |
    And producer "orders" submits batch "second" with rows
      | region | order_id | amount |
      | eu     | o-2      | 2      |
    Then HTTP receiver "sink" eventually receives at least 1 request
    # The window of two is full, so the owner queues the next two batches without admitting them.
    When producer "orders" submits batch "third" with rows
      | region | order_id | amount |
      | us     | o-3      | 3      |
    And producer "orders" submits batch "fourth" with rows
      | region | order_id | amount |
      | us     | o-4      | 4      |
    Then within "30s" the server process cluster describes ingestor "orders_in" with
      """
      owner: {{owner}}
      forwarded producers: 1
      outstanding batches: 4
      admitted batches: 2
      """
    When server process node "{{owner}}" receives SIGKILL
    Then server process node "{{owner}}" is terminated by SIGKILL
    And batch "first" has an unknown outcome because "owner_lost"
    And batch "second" has an unknown outcome because "owner_lost"
    And batch "third" is not admitted because "producer ended"
    And batch "fourth" is not admitted because "producer ended"
    When HTTP receiver "sink" releases its held responses with "respond 200"
    And server process node "{{owner}}" restarts from its existing database
    And these NSPL commands are executed on the server process cluster
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{owner}} IGNORE PREFERENCES;
      """
    Then within "60s" the server process cluster describes ingestor "orders_in" with
      """
      owner: {{owner}}
      """
    When producer "orders" submits batch "fifth" with rows
      | region | order_id | amount |
      | us     | o-5      | 5      |
    Then batch "fifth" completes
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/eu/o-1
      """
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/us/o-5
      """
    And HTTP receiver "sink" captured no request with request line "POST /orders/us/o-3"
    And HTTP receiver "sink" captured no request with request line "POST /orders/us/o-4"

  Scenario: Killing a forwarding node leaves its batches uncertain while the producer restores
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "hold response until released"
    And a 3 node nervix-server process cluster is started
    And the server process cluster is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (region STRING, order_id STRING, amount I64);
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And the server process cluster leader is saved as placeholder "leader"
    And a server process cluster node other than placeholder "leader" is saved as placeholder "owner"
    And a server process cluster node other than placeholders "leader" and "owner" is saved as placeholder "entry"
    And the server process cluster is configured with these NSPL commands
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{owner}} IGNORE PREFERENCES;
      RELOCATE RELAY orders ONTO NODE {{leader}} IGNORE PREFERENCES;
      RELOCATE EMITTER published ONTO NODE {{leader}} IGNORE PREFERENCES;
      """
    And client "app" is connected to server process node "{{entry}}"
    Then within "30s" the server process cluster describes ingestor "orders_in" with
      """
      owner: {{owner}}
      """
    When within "60s" client "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64"
    And producer "orders" submits batch "first" with rows
      | region | order_id | amount |
      | eu     | o-1      | 1      |
    And producer "orders" submits batch "second" with rows
      | region | order_id | amount |
      | eu     | o-2      | 2      |
    Then HTTP receiver "sink" eventually receives at least 1 request
    When producer "orders" submits batch "third" with rows
      | region | order_id | amount |
      | us     | o-3      | 3      |
    Then within "30s" the server process cluster describes ingestor "orders_in" with
      """
      owner: {{owner}}
      forwarded producers: 1
      outstanding batches: 3
      admitted batches: 2
      """
    When server process node "{{entry}}" receives SIGKILL
    Then server process node "{{entry}}" is terminated by SIGKILL
    And batch "first" has an unknown outcome because "session_lost"
    And batch "second" has an unknown outcome because "session_lost"
    And batch "third" has an unknown outcome because "session_lost"
    # The owner detaches the producer and drops the batch it had only queued.
    And within "60s" the server process cluster describes ingestor "orders_in" with
      """
      producers: 0
      outstanding batches: 0
      admitted batches: 2
      """
    # Both admitted batches still reach the sink and leave the window.
    When HTTP receiver "sink" releases its held responses with "respond 200"
    Then HTTP receiver "sink" eventually receives at least 2 requests
    And within "30s" the server process cluster describes ingestor "orders_in" with
      """
      admitted batches: 0
      """
    When producer "orders" submits batch "fourth" with rows
      | region | order_id | amount |
      | us     | o-4      | 4      |
    Then batch "fourth" completes
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/eu/o-1
      """
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/eu/o-2
      """
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/us/o-4
      """
    And HTTP receiver "sink" captured no request with request line "POST /orders/us/o-3"

  Scenario: Freezing an ingestor owner makes admitted batches uncertain and restores the producer
    Given HTTP receiver "sink" is running
    And HTTP receiver "sink" answers unscripted requests with "hold response until released"
    And a 3 node nervix-server process cluster is started
    And the server process cluster is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (region STRING, order_id STRING, amount I64);
      CREATE RELAY orders SCHEMA order_in UNBRANCHED;
      CREATE INGESTOR orders_in
        FROM CLIENT SCHEMA order_in
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.sink}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM orders
        TO HTTP sink_api
          METHOD 'POST' PATH concat('/orders/', input.region, '/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And the server process cluster leader is saved as placeholder "leader"
    And a server process cluster node other than placeholder "leader" is saved as placeholder "owner"
    And a server process cluster node other than placeholders "leader" and "owner" is saved as placeholder "entry"
    And the server process cluster is configured with these NSPL commands
      """
      RELOCATE INGESTOR orders_in ONTO NODE {{owner}} IGNORE PREFERENCES;
      RELOCATE RELAY orders ONTO NODE {{leader}} IGNORE PREFERENCES;
      RELOCATE EMITTER published ONTO NODE {{leader}} IGNORE PREFERENCES;
      """
    And client "app" is connected to server process node "{{entry}}"
    Then within "30s" the server process cluster describes ingestor "orders_in" with
      """
      owner: {{owner}}
      """
    When within "60s" client "app" opens producer "orders" on ingestor "orders_in" expecting fields "region STRING, order_id STRING, amount I64"
    And producer "orders" submits batch "first" with rows
      | region | order_id | amount |
      | eu     | o-1      | 1      |
    And producer "orders" submits batch "second" with rows
      | region | order_id | amount |
      | eu     | o-2      | 2      |
    Then HTTP receiver "sink" eventually receives at least 1 request
    When producer "orders" submits batch "third" with rows
      | region | order_id | amount |
      | us     | o-3      | 3      |
    Then within "30s" the server process cluster describes ingestor "orders_in" with
      """
      owner: {{owner}}
      forwarded producers: 1
      outstanding batches: 3
      admitted batches: 2
      """
    # A stopped process keeps its connections open and answers nothing, so no reset ends the link.
    When server process node "{{owner}}" receives SIGSTOP
    Then batch "first" has an unknown outcome because "owner_lost"
    And batch "second" has an unknown outcome because "owner_lost"
    And batch "third" is not admitted because "producer ended"
    When server process node "{{owner}}" receives SIGCONT
    And HTTP receiver "sink" releases its held responses with "respond 200"
    Then within "60s" the server process cluster describes ingestor "orders_in" with
      """
      producers: 0
      admitted batches: 0
      """
    When producer "orders" submits batch "fourth" with rows
      | region | order_id | amount |
      | us     | o-4      | 4      |
    Then batch "fourth" completes
    And HTTP receiver "sink" captured one request that is
      """
      POST /orders/us/o-4
      """
    And HTTP receiver "sink" captured no request with request line "POST /orders/us/o-3"

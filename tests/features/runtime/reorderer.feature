Feature: Reorderer
  Scenario Outline: Reorderer emits records ordered by BY expressions with arrival tie-breaks
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        sequence I64,
        payload STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        sequence integer,
        payload string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_http_notifications BY tenant_branch TTL 5m;
        CREATE RELAY incoming_notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE RELAY ordered_notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO incoming_notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { tenant = incoming_notifications.tenant }
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE REORDERER order_notifications
        FROM incoming_notifications
        TO ordered_notifications BRANCHED BY by_http_notifications
        BY incoming_notifications.sequence
        MAX TIME 10s
        FLUSH EACH 2s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        SUBSCRIBE SESSION TO ordered_notifications WHERE ordered_notifications.tenant = 'acme';
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","sequence":3,"payload":"third"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","sequence":1,"payload":"first"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","sequence":2,"payload":"second"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "payload":"first"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    Then the relay subscription receives a payload
      """
      "payload":"second"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    Then the relay subscription receives a payload
      """
      "payload":"third"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Reorderer evaluates BY function calls through the VM
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        category STRING,
        priority I32,
        payload STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        category string,
        priority integer,
        payload string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_http_notifications BY tenant_branch TTL 5m;
        CREATE RELAY incoming_notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE RELAY ordered_notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO incoming_notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { tenant = incoming_notifications.tenant }
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE REORDERER order_notifications
        FROM incoming_notifications
        TO ordered_notifications BRANCHED BY by_http_notifications
        BY lower(trim(incoming_notifications.category)), abs(incoming_notifications.priority)
        MAX TIME 10s
        FLUSH EACH 2s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        SUBSCRIBE SESSION TO ordered_notifications WHERE ordered_notifications.tenant = 'acme';
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","category":" B ","priority":-1,"payload":"b-low"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","category":"a","priority":-5,"payload":"a-high"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","category":" A ","priority":-2,"payload":"a-low"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","category":"b","priority":-3,"payload":"b-high"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "payload":"a-low"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    Then the relay subscription receives a payload
      """
      "payload":"a-high"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    Then the relay subscription receives a payload
      """
      "payload":"b-low"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    Then the relay subscription receives a payload
      """
      "payload":"b-high"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario: Reorderer rejects global error policy
    Given a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "expected ; | end of input, found ON"
      """
      CREATE SCHEMA notification (
        tenant STRING,
        sequence I64
      );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_order_notifications BY tenant_branch TTL 5m;
        CREATE RELAY incoming_notifications SCHEMA notification BRANCHED BY by_order_notifications;
        CREATE RELAY ordered_notifications SCHEMA notification BRANCHED BY by_order_notifications;
        CREATE REORDERER order_notifications
        FROM incoming_notifications
        TO ordered_notifications BRANCHED BY by_order_notifications
        BY incoming_notifications.sequence
        MAX TIME 10s
        FLUSH EACH 2s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """

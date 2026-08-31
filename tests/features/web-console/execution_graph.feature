Feature: Web console execution graph
  Scenario: Execution graph draws an unbranched pipeline as a single straight line
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_record (
        order_id STRING,
        amount I64
      );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT (
        order_id string,
        amount integer
      );
      CREATE CODEC order_codec FROM WIRE JSON SCHEMA order_wire TO SCHEMA order_record;
      CREATE RELAY orders SCHEMA order_record UNBRANCHED;
      CREATE CLIENT kafka_in TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT kafka_out TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9093' };
      CREATE INGESTOR orders_in
        FROM KAFKA kafka_in TOPIC orders_topic OFFSET BY CONSUMER GROUP orders_group INSTANCES 1 MODE NO_ACK PARALLEL
        ON QUIESCE SUSPEND DECODE USING order_codec
        TO orders
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER orders_out FROM orders TO KAFKA kafka_out TOPIC orders_out_topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING order_codec
        INHERIT ALL
        FLUSH EACH 1s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "orders_in"
    And graph items "kafka_in, orders_in, orders, orders_out, kafka_out" are horizontally aligned
    And graph item "kafka_in" is left of graph item "orders_in"
    And graph item "orders_in" is left of graph item "orders"
    And graph item "orders" is left of graph item "orders_out"
    And graph item "orders_out" is left of graph item "kafka_out"
    And graph edge from "orders_in" to "orders" starts horizontally
    And graph edge from "orders_in" to "orders" ends horizontally
    And graph edge from "orders_in" to "orders" has at most 0 rounded turns
    And no graph edge crosses any graph item
    And no graph badge overlaps another graph item or badge

  Scenario: Execution graph keeps fan-out clear of items and gives each route its own port
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_record (
        order_id STRING,
        amount I64
      );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT (
        order_id string,
        amount integer
      );
      CREATE CODEC order_codec FROM WIRE JSON SCHEMA order_wire TO SCHEMA order_record;
      CREATE RELAY orders SCHEMA order_record UNBRANCHED;
      CREATE RELAY high_value_orders SCHEMA order_record UNBRANCHED;
      CREATE RELAY routine_orders SCHEMA order_record UNBRANCHED;
      CREATE CLIENT kafka_in TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT kafka_out TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9093' };
      CREATE INGESTOR orders_in
        FROM KAFKA kafka_in TOPIC orders_topic OFFSET BY CONSUMER GROUP orders_group INSTANCES 1 MODE NO_ACK PARALLEL
        ON QUIESCE SUSPEND DECODE USING order_codec
        TO orders
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_orders FROM orders
        UNBRANCHED
        TO high_value_orders
        INHERIT ALL
        WHERE input.amount > 1000
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG
        TO routine_orders
        INHERIT ALL
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG;
      CREATE EMITTER high_value_out FROM high_value_orders TO KAFKA kafka_out TOPIC high_value_topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING order_codec
        INHERIT ALL
        FLUSH EACH 1s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "route_orders"
    And graph item "orders" is left of graph item "route_orders"
    And graph item "route_orders" is left of graph item "high_value_orders"
    And graph edge from "route_orders" to "high_value_orders" is visible
    And graph edge from "route_orders" to "routine_orders" is visible
    And graph edge from "route_orders" to "high_value_orders" departs at a different port than graph edge from "route_orders" to "routine_orders"
    And no graph edge crosses any graph item
    And no graph badge overlaps another graph item or badge
    And graph edge from "route_orders" to "high_value_orders" does not intersect graph edge from "route_orders" to "routine_orders"

  Scenario: Execution graph contains a branch group and names its branch and key fields
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        user_id integer
      );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE SCHEMA tenant_user_key ( tenant STRING, user_id I64 );
      CREATE BRANCH by_tenant_user SCHEMA tenant_user_key TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_tenant_user;
      CREATE RELAY validated_notifications SCHEMA notification BRANCHED BY by_tenant_user;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_ingress ON edge PATH '/notifications' TYPE HTTP;
      CREATE INGESTOR notifications_source
        FROM ENDPOINT notifications_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_tenant_user
        SET tenant = message.tenant, user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR notification_forwarder FROM notifications
        DEDUPLICATE ON input.tenant, input.user_id
        MAX TIME 10m
        BRANCHED BY by_tenant_user
        TO validated_notifications
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "notification_forwarder"
    And branch group "by_tenant_user" header shows key fields "tenant, user_id"
    And branch group "by_tenant_user" contains graph item "notifications"
    And branch group "by_tenant_user" contains graph item "notification_forwarder"
    And branch group "by_tenant_user" contains graph item "validated_notifications"
    And branch group "by_tenant_user" does not contain graph item "notifications_source"
    And no graph edge crosses any graph item

  Scenario: Execution graph marks a reingestor feedback loop as a return path
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA retry_record (
        attempt I64,
        payload STRING
      );
      CREATE WIRE JSON SCHEMA retry_wire MODE STRICT (
        attempt integer,
        payload string
      );
      CREATE CODEC retry_codec FROM WIRE JSON SCHEMA retry_wire TO SCHEMA retry_record;
      CREATE RELAY pending SCHEMA retry_record UNBRANCHED;
      CREATE RELAY retried SCHEMA retry_record UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT retry_ingress ON edge PATH '/retries' TYPE HTTP;
      CREATE INGESTOR retry_source
        FROM ENDPOINT retry_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING retry_codec
        TO pending
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION retry_router FROM pending
        UNBRANCHED
        TO retried
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      CREATE REINGESTOR retry_loop FROM retried
        TO pending
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "retry_loop"
    And graph edge from "retry_loop" to "pending" is a return path
    And graph item "pending" is left of graph item "retry_router"
    And graph item "retry_router" is left of graph item "retry_loop"
    And no graph edge crosses any graph item

  Scenario: Execution graph reports its framing, feed state and every node kind
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_record (
        order_id STRING,
        amount I64
      );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT (
        order_id string,
        amount integer
      );
      CREATE CODEC order_codec FROM WIRE JSON SCHEMA order_wire TO SCHEMA order_record;
      CREATE RELAY orders SCHEMA order_record UNBRANCHED;
      CREATE CLIENT kafka_in TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE INGESTOR orders_in
        FROM KAFKA kafka_in TOPIC orders_topic OFFSET BY CONSUMER GROUP orders_group INSTANCES 1 MODE NO_ACK PARALLEL
        ON QUIESCE SUSPEND DECODE USING order_codec
        TO orders
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 250ms MAX BATCH SIZE 512kb
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".legend-row" contains "Ingestor"
    And selector ".legend-row" contains "Processor"
    And selector ".legend-row" contains "Emitter"
    And selector ".legend-row" contains "Relay"
    And selector ".legend-row" contains "Client"
    And selector ".graph-title [data-lifecycle]" contains "STOPPED"
    And selector ".graph-title [data-freshness]" contains "LIVE"
    And the whole graph is visible in the graph viewport
    And graph zoom stays within 25 and 300 percent
    And graph geometry does not change while snapshots arrive

Feature: Relay correlation
  Scenario Outline: Correlator matches records inside one branch across independent output routes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA left_profile (
        tenant STRING,
        first_name STRING,
        marker I64
      );
        CREATE SCHEMA right_profile (
        tenant STRING,
        first_name STRING,
        surname STRING
      );
      CREATE SCHEMA correlated_profile (
        tenant STRING,
        normalized_name STRING,
        left_marker I64,
        surname STRING,
        memo STRING OPTIONAL
      );
        CREATE SCHEMA correlation_audit (
        tenant STRING,
        audit_name STRING
      );
        CREATE WIRE JSON SCHEMA left_profile_wire MODE STRICT (
        tenant string,
        first_name string,
        marker integer
      );
        CREATE WIRE JSON SCHEMA right_profile_wire MODE STRICT (
        tenant string,
        first_name string,
        surname string
      );
        CREATE WIRE JSON CODEC left_profile_codec
        FROM WIRE SCHEMA left_profile_wire
        TO SCHEMA left_profile;
        CREATE WIRE JSON CODEC right_profile_codec
        FROM WIRE SCHEMA right_profile_wire
        TO SCHEMA right_profile;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_left_profile_ingestor SCHEMA tenant_branch TTL 5m;
        CREATE RELAY left_profiles SCHEMA left_profile BRANCHED BY by_left_profile_ingestor;
        CREATE RELAY left_profile_aliases SCHEMA left_profile BRANCHED BY by_left_profile_ingestor;
        CREATE RELAY right_profiles SCHEMA right_profile BRANCHED BY by_left_profile_ingestor;
        CREATE RELAY right_profile_aliases SCHEMA right_profile BRANCHED BY by_left_profile_ingestor;
        CREATE RELAY correlated_profiles SCHEMA correlated_profile BRANCHED BY by_left_profile_ingestor;
        CREATE RELAY correlation_audits SCHEMA correlation_audit BRANCHED BY by_left_profile_ingestor;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE HTTP ENDPOINT left_ingress
        ON edge
        PATH '/left';
        CREATE HTTP ENDPOINT left_alias_ingress
        ON edge
        PATH '/left-alias';
        CREATE HTTP ENDPOINT right_ingress
        ON edge
        PATH '/right';
        CREATE HTTP ENDPOINT right_alias_ingress
        ON edge
        PATH '/right-alias';
        CREATE HTTP ENDPOINT INGESTOR left_profile_ingestor
        FROM left_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING left_profile_codec
        TO left_profiles
        INHERIT ALL
        BRANCHED BY by_left_profile_ingestor
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE HTTP ENDPOINT INGESTOR left_profile_alias_ingestor
        FROM left_alias_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING left_profile_codec
        TO left_profile_aliases
        INHERIT ALL
        BRANCHED BY by_left_profile_ingestor
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE HTTP ENDPOINT INGESTOR right_profile_ingestor
        FROM right_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING right_profile_codec
        TO right_profiles
        INHERIT ALL
        BRANCHED BY by_left_profile_ingestor
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE HTTP ENDPOINT INGESTOR right_profile_alias_ingestor
        FROM right_alias_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING right_profile_codec
        TO right_profile_aliases
        INHERIT ALL
        BRANCHED BY by_left_profile_ingestor
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CORRELATOR correlate_profiles
        LEFT FROM left_profiles WHERE left.marker > 0,
                  left_profile_aliases WHERE left.marker > 0
        RIGHT FROM right_profiles WHERE right.tenant = 'acme',
                   right_profile_aliases WHERE right.tenant = 'acme'
        CORRELATE WHERE lower(left.first_name) = lower(right.first_name)
        MATCH <match_policy>
        MAX TIME 2m
        ON CORRELATION TIMEOUT DROP, DROP
        BRANCHED BY by_left_profile_ingestor
        TO correlated_profiles
        SET tenant = left.tenant,
          normalized_name = lower(left.first_name),
          left_marker = left.marker,
          surname = upper(right.surname),
          memo = NULL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        TO correlation_audits
        SET tenant = right.tenant,
          audit_name = concat(upper(left.first_name), ' ', upper(right.surname))
        WHERE right.surname != 'private'
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION correlated_profiles_subscription TO correlated_profiles WHERE tenant = 'acme';
        CREATE SUBSCRIPTION correlation_audits_subscription TO correlation_audits WHERE tenant = 'acme';
        START;
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CORRELATOR correlate_profiles;
      """
    Then the last command output owner is saved as placeholder "correlator_owner"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"acme","first_name":"John","marker":1}
      """
    Then within "30s" node "{{correlator_owner}}" observability metric "nervix_messages_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      target_kind="CORRELATOR"
      target="correlate_profiles"
      direction="received"
      relay="left_profiles"
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left-alias"
      """
      {"tenant":"acme","first_name":"JOHN","marker":2}
      """
    Then within "30s" node "{{correlator_owner}}" observability metric "nervix_messages_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      target_kind="CORRELATOR"
      target="correlate_profiles"
      direction="received"
      relay="left_profile_aliases"
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"acme","first_name":"Jane","marker":3}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right"
      """
      {"tenant":"beta","first_name":"john","surname":"wrong"}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right-alias"
      """
      {"tenant":"acme","first_name":"john","surname":"smith"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "left_marker":<expected_left_marker> | "normalized_name":"john" | "surname":"SMITH"
      "audit_name":"JOHN SMITH"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload does not contain "memo\""
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right-alias"
      """
      {"tenant":"acme","first_name":"jane","surname":"doe"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "left_marker":3 | "normalized_name":"jane" | "surname":"DOE"
      "audit_name":"JANE DOE"
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"acme","first_name":"Eve","marker":4}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right"
      """
      {"tenant":"acme","first_name":"eve","surname":"private"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "left_marker":4 | "normalized_name":"eve" | "surname":"PRIVATE"
      """
    And the relay subscription does not receive a payload within "500ms"

    Examples:
      | cluster_size | replica_count | match_policy | expected_left_marker |
      | 1            | 0             | EARLIEST     | 1                    |
      | 3            | 0             | EARLIEST     | 1                    |
      | 3            | 1             | EARLIEST     | 1                    |
      | 1            | 0             | LATEST       | 2                    |
      | 3            | 0             | LATEST       | 2                    |
      | 3            | 1             | LATEST       | 2                    |

  @correlator_timeout_send_to
  Scenario Outline: Correlator sends each side's unmatched records to that side's timeout relay once MAX TIME passes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA settle_order (tenant STRING, order_id STRING, item STRING);
      CREATE SCHEMA settle_payment (tenant STRING, order_id STRING, amount I64);
      CREATE SCHEMA settled_order (tenant STRING, order_id STRING, item STRING, amount I64);
      CREATE SCHEMA settle_tenant (tenant STRING);
      CREATE BRANCH settle_by_tenant SCHEMA settle_tenant TTL 5m;
      CREATE JAQ JSON CODEC settle_order_codec TO SCHEMA settle_order
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE JAQ JSON CODEC settle_payment_codec TO SCHEMA settle_payment
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY settle_orders SCHEMA settle_order BRANCHED BY settle_by_tenant;
      CREATE RELAY settle_payments SCHEMA settle_payment BRANCHED BY settle_by_tenant;
      CREATE RELAY settled_orders SCHEMA settled_order BRANCHED BY settle_by_tenant;
      CREATE RELAY unpaid_orders SCHEMA settle_order BRANCHED BY settle_by_tenant;
      CREATE RELAY unclaimed_payments SCHEMA settle_payment BRANCHED BY settle_by_tenant;
      CREATE VHOST settle_host settle-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT settle_orders_endpoint ON settle_host PATH '/orders';
      CREATE HTTP ENDPOINT settle_payments_endpoint ON settle_host PATH '/payments';
      CREATE HTTP ENDPOINT INGESTOR settle_orders_ingestor FROM settle_orders_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING settle_order_codec
        TO settle_orders INHERIT ALL BRANCHED BY settle_by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR settle_payments_ingestor FROM settle_payments_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING settle_payment_codec
        TO settle_payments INHERIT ALL BRANCHED BY settle_by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CORRELATOR settle_payments_to_orders
        LEFT FROM settle_orders
        RIGHT FROM settle_payments
        ATTACHED
        CORRELATE WHERE left.order_id = right.order_id
        MATCH EARLIEST
        MAX TIME 2s
        ON CORRELATION TIMEOUT SEND TO unpaid_orders, SEND TO unclaimed_payments
        BRANCHED BY settle_by_tenant
        TO settled_orders
          SET tenant = left.tenant,
              order_id = left.order_id,
              item = left.item,
              amount = right.amount
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION settled_orders_subscription TO settled_orders;
      CREATE SUBSCRIPTION unpaid_orders_subscription TO unpaid_orders;
      CREATE SUBSCRIPTION unclaimed_payments_subscription TO unclaimed_payments;
      START;
      """
    # Both tenants order o1; tenant a's o2 is never paid and tenant b's payment for o9 has no order.
    And http payload is posted to host "settle-{{test_id}}.example.com" path "/orders"
      """
      [
        {"tenant":"a","order_id":"o1","item":"lamp"},
        {"tenant":"b","order_id":"o1","item":"desk"},
        {"tenant":"a","order_id":"o2","item":"rug"}
      ]
      """
    And http payload is posted to host "settle-{{test_id}}.example.com" path "/payments"
      """
      [
        {"tenant":"b","order_id":"o1","amount":200},
        {"tenant":"a","order_id":"o1","amount":100},
        {"tenant":"b","order_id":"o9","amount":50}
      ]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"a" | "order_id":"o1" | "item":"lamp" | "amount":100
      "tenant":"b" | "order_id":"o1" | "item":"desk" | "amount":200
      "tenant":"a" | "order_id":"o2" | "item":"rug"
      "tenant":"b" | "order_id":"o9" | "amount":50
      """
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

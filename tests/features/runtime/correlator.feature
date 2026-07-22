Feature: Relay correlation
  Scenario Outline: Correlator matches records inside one branch using the selected pending record
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
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
        CREATE STRICT WIRE JSON SCHEMA left_profile_wire (
        tenant string,
        first_name string,
        marker integer
      );
        CREATE STRICT WIRE JSON SCHEMA right_profile_wire (
        tenant string,
        first_name string,
        surname string
      );
        CREATE CODEC left_profile_codec
        FROM WIRE JSON SCHEMA left_profile_wire
        TO SCHEMA left_profile;
        CREATE CODEC right_profile_codec
        FROM WIRE JSON SCHEMA right_profile_wire
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
        CREATE ENDPOINT left_ingress
        ON edge
        PATH '/left'
        TYPE HTTP;
        CREATE ENDPOINT left_alias_ingress
        ON edge
        PATH '/left-alias'
        TYPE HTTP;
        CREATE ENDPOINT right_ingress
        ON edge
        PATH '/right'
        TYPE HTTP;
        CREATE ENDPOINT right_alias_ingress
        ON edge
        PATH '/right-alias'
        TYPE HTTP;
        CREATE INGESTOR left_profile_ingestor
        FROM ENDPOINT left_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING left_profile_codec
        TO left_profiles
        INHERIT ALL
        BRANCHED BY by_left_profile_ingestor
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR left_profile_alias_ingestor
        FROM ENDPOINT left_alias_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING left_profile_codec
        TO left_profile_aliases
        INHERIT ALL
        BRANCHED BY by_left_profile_ingestor
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR right_profile_ingestor
        FROM ENDPOINT right_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING right_profile_codec
        TO right_profiles
        INHERIT ALL
        BRANCHED BY by_left_profile_ingestor
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR right_profile_alias_ingestor
        FROM ENDPOINT right_alias_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING right_profile_codec
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
        MAX TIME 5s
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
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION correlated_profiles_subscription TO correlated_profiles WHERE tenant = 'acme';
        CREATE SUBSCRIPTION correlation_audits_subscription TO correlation_audits WHERE tenant = 'acme';
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"acme","first_name":"John","marker":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left-alias"
      """
      {"tenant":"acme","first_name":"JOHN","marker":2}
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

    Examples:
      | cluster_size | replica_count | match_policy | expected_left_marker |
      | 1            | 0             | EARLIEST     | 1                    |
      | 3            | 0             | EARLIEST     | 1                    |
      | 3            | 1             | EARLIEST     | 1                    |
      | 1            | 0             | LATEST       | 2                    |
      | 3            | 0             | LATEST       | 2                    |
      | 3            | 1             | LATEST       | 2                    |

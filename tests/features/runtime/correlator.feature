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
      CREATE RELAY left_profiles SCHEMA left_profile PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY right_profiles SCHEMA right_profile PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY correlated_profiles SCHEMA correlated_profile PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT left_ingress
        ON edge
        PATH '/left'
        TYPE HTTP;

      CREATE ENDPOINT right_ingress
        ON edge
        PATH '/right'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR left_profile_ingestor
        TO left_profiles
        DECODE USING left_profile_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = left_profiles.tenant } TTL 5m
        FLUSH IMMEDIATE
        FROM ENDPOINT left_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR right_profile_ingestor
        TO right_profiles
        DECODE USING right_profile_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = right_profiles.tenant } TTL 5m
        FLUSH IMMEDIATE
        FROM ENDPOINT right_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE CORRELATOR correlate_profiles
        FROM left_profiles, right_profiles
        CORRELATE WHERE lower(left_profiles.first_name) = lower(right_profiles.first_name)
        MATCH <match_policy>
        TO correlated_profiles PARAMETERIZED BY tenant_branch
        FLUSH IMMEDIATE
        OUTPUT
          correlated_profiles.tenant = left_profiles.tenant,
          correlated_profiles.normalized_name = lower(left_profiles.first_name),
          correlated_profiles.left_marker = left_profiles.marker,
          correlated_profiles.surname = upper(right_profiles.surname),
          correlated_profiles.memo = NULL
        MAX TIME 5s
        ON CORRELATION TIMEOUT DROP, DROP
        ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO correlated_profiles WHERE correlated_profiles.tenant = 'acme';
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"acme","first_name":"John","marker":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"acme","first_name":"JOHN","marker":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right"
      """
      {"tenant":"beta","first_name":"john","surname":"wrong"}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right"
      """
      {"tenant":"acme","first_name":"john","surname":"smith"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "left_marker":<expected_left_marker>
      """
    And the last relay subscription payload contains
      """
      "normalized_name":"john"
      "surname":"SMITH"
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

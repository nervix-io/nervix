Feature: Typed state inspection through the native host libraries
  Python, Ruby and Java read the same existing envelope through their native library, keeping
  page metadata and refusals distinct and retaining the inspection after its outcome and session.

  @native_binding_inspection @client_conformance_toolchain
  Scenario Outline: A <runtime> client retains typed inspection and refusal on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA inspection_row ( value I64 );
      CREATE RELAY events SCHEMA inspection_row UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY plain SCHEMA inspection_row UNBRANCHED;
      CREATE SCHEMA inspected ( tenant STRING SENSITIVE, secret STRING SENSITIVE, sequence I64 );
      CREATE WIRE JSON SCHEMA inspected_wire MODE STRICT ( tenant string, secret string, sequence integer );
      CREATE WIRE JSON CODEC inspected_codec FROM WIRE SCHEMA inspected_wire TO SCHEMA inspected;
      CREATE SCHEMA inspected_key ( tenant STRING SENSITIVE );
      CREATE BRANCH by_tenant SCHEMA inspected_key TTL 5m;
      CREATE RELAY populated SCHEMA inspected BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST inspection_edge native-inspection-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT inspection_ingress ON inspection_edge PATH '/rows';
      CREATE HTTP ENDPOINT INGESTOR inspected_in FROM inspection_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING inspected_codec
        TO populated INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION inspection_evidence TO populated;
      START;
      """
    When http payload is posted to node "node-1" with host "native-inspection-{{test_id}}.example.com" path "/rows"
      """
      {"tenant":"alpha-private","secret":"first-private","sequence":1}
      """
    And http payload is posted to node "node-1" with host "native-inspection-{{test_id}}.example.com" path "/rows"
      """
      {"tenant":"beta-private","secret":"second-private","sequence":2}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "sequence":1
      "sequence":2
      """
    When the "<runtime>" client probe inspects typed state on a follower when available
    Then within "180s" the client probe reports
      """
      INSPECTION page rows=0
      INSPECTION refused not_materialized
      INSPECTION branched rows=2 pages=2
      INSPECTION retained
      PASS
      """

    @client_probe_python
    Examples: Python
      | runtime | cluster_size |
      | python  | 1            |
      | python  | 3            |

    @client_probe_ruby
    Examples: Ruby
      | runtime | cluster_size |
      | ruby    | 1            |
      | ruby    | 3            |

    @client_probe_java
    Examples: Java
      | runtime | cluster_size |
      | java    | 1            |
      | java    | 3            |

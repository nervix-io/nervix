@graph_loops
Feature: A domain's graph has no loops
  Rows flow from ingestors through relays and processors to emitters, and never come back to a
  relay they already passed. A change that would let data travel in a circle, through a route, an
  error route, a correlator's timeout or a read of a relay's materialized state, is refused before
  any of it applies, and the refusal names the loop in the order data would travel it, starting at
  its first relay.

  Scenario Outline: Statements that would close a loop are refused and create nothing
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA rec ( k STRING, v I64 );
      CREATE RELAY a SCHEMA rec UNBRANCHED;
      CREATE RELAY b SCHEMA rec UNBRANCHED;
      CREATE RELAY c SCHEMA rec UNBRANCHED;
      CREATE RELAY sa SCHEMA rec UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      """
    # A node that writes into the relay it reads.
    When these Plexus commands fail with "the graph of domain '{{domain}}' would contain a loop: RELAY 'a' → JUNCTION 'self_junction' → RELAY 'a'"
      """
      CREATE JUNCTION self_junction FROM a UNBRANCHED
        TO a INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "the graph of domain '{{domain}}' would contain a loop: RELAY 'a' → REINGESTOR 'self_reingestor' → RELAY 'a'"
      """
      CREATE REINGESTOR self_reingestor FROM a
        TO a INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    # A node whose error route sends a failed row back into the relay it reads.
    And these Plexus commands fail with "the graph of domain '{{domain}}' would contain a loop: RELAY 'a' → JUNCTION 'error_junction' →[error route] RELAY 'a'"
      """
      CREATE JUNCTION error_junction FROM a UNBRANCHED
        TO b INHERIT ALL FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO a SET k = input.k, v = input.v;
      """
    # A correlator whose timeout sends an unmatched row back into its own left input.
    And these Plexus commands fail with "the graph of domain '{{domain}}' would contain a loop: RELAY 'a' → CORRELATOR 'timeout_correlator' →[correlation timeout] RELAY 'a'"
      """
      CREATE CORRELATOR timeout_correlator
        LEFT FROM a
        RIGHT FROM b
        CORRELATE WHERE left.k = right.k
        MATCH EARLIEST
        MAX TIME 30s
        ON CORRELATION TIMEOUT SEND TO a, DROP
        UNBRANCHED
        TO c SET k = left.k, v = right.v FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    # A node that writes into the relay whose state it reads.
    And these Plexus commands fail with "the graph of domain '{{domain}}' would contain a loop: RELAY 'sa' →[state read] JUNCTION 'state_junction' → RELAY 'sa'"
      """
      CREATE JUNCTION state_junction FROM a UNBRANCHED
        USING MATERIALIZED STATE sa REQUIRED SKIP
        TO sa INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "the graph of domain '{{domain}}' would contain a loop: RELAY 'sa' →[state read] GENERATOR 'state_generator' → RELAY 'sa'"
      """
      CREATE GENERATOR state_generator
        USING MATERIALIZED STATE sa
        EACH 1s
        UNBRANCHED
        TO sa SET k = relay_state.sa.k, v = relay_state.sa.v
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    # Two nodes that feed each other, created together: the transaction is refused as a whole.
    And these Plexus commands fail with "the graph of domain '{{domain}}' would contain a loop: RELAY 'a' → JUNCTION 'forward' → RELAY 'b' → JUNCTION 'backward' → RELAY 'a'"
      """
      BEGIN;
      CREATE JUNCTION forward FROM a UNBRANCHED
        TO b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION backward FROM b UNBRANCHED
        TO a INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      COMMIT;
      """
    And these Plexus commands fail with "the graph of domain '{{domain}}' would contain a loop: RELAY 'a' → REINGESTOR 'ring_out' → RELAY 'b' → REINGESTOR 'ring_back' → RELAY 'a'"
      """
      BEGIN;
      CREATE REINGESTOR ring_out FROM a
        TO b INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE REINGESTOR ring_back FROM b
        TO a INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      COMMIT;
      """
    # Nothing of the refused statements was created: every name is still free.
    When these Plexus commands are executed on the leader node
      """
      CREATE JUNCTION self_junction FROM a UNBRANCHED
        TO c INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE REINGESTOR self_reingestor FROM a
        TO c INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION error_junction FROM a UNBRANCHED
        TO b INHERIT ALL FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO c SET k = input.k, v = input.v;
      CREATE CORRELATOR timeout_correlator
        LEFT FROM a
        RIGHT FROM b
        CORRELATE WHERE left.k = right.k
        MATCH EARLIEST
        MAX TIME 30s
        ON CORRELATION TIMEOUT SEND TO c, DROP
        UNBRANCHED
        TO c SET k = left.k, v = right.v FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION state_junction FROM a UNBRANCHED
        USING MATERIALIZED STATE sa REQUIRED SKIP
        TO c INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE GENERATOR state_generator
        USING MATERIALIZED STATE sa
        EACH 1s
        UNBRANCHED
        TO c SET k = relay_state.sa.k, v = relay_state.sa.v
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION forward FROM a UNBRANCHED
        TO b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION backward FROM b UNBRANCHED
        TO c INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE REINGESTOR ring_out FROM a
        TO b INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE REINGESTOR ring_back FROM b
        TO c INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An alteration that would close a loop is refused and the running graph keeps working
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA rec ( k STRING, v I64 );
      CREATE WIRE JSON SCHEMA rec_wire MODE STRICT ( k string, v integer );
      CREATE WIRE JSON CODEC rec_codec FROM WIRE SCHEMA rec_wire TO SCHEMA rec;
      CREATE RELAY a SCHEMA rec UNBRANCHED;
      CREATE RELAY b SCHEMA rec UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-graph-loops.example.com;
      CREATE HTTP ENDPOINT loop_ingress ON edge PATH '/records';
      CREATE HTTP ENDPOINT INGESTOR loop_ingestor
        FROM loop_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING rec_codec
        TO a INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION forward FROM a UNBRANCHED
        TO b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION b_subscription TO b;
      START;
      """
    And http payload is posted to host "http-{{test_id}}-graph-loops.example.com" path "/records"
      """
      {"k":"before","v":1}
      """
    Then the relay subscription receives a payload
      """
      "k":"before"
      """
    # The subscription's session stays open while the refused alteration runs on its own session.
    When these Plexus commands are attempted on node "node-1"
      """
      ALTER JUNCTION forward ADD ROUTE TO a INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command error contains
      """
      the graph of domain '{{domain}}' would contain a loop: RELAY 'a' → JUNCTION 'forward' → RELAY 'a'
      """
    When http payload is posted to host "http-{{test_id}}-graph-loops.example.com" path "/records"
      """
      {"k":"after","v":2}
      """
    Then the relay subscription receives a payload
      """
      "k":"after"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

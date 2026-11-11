@reference_completion
Feature: CLI completion offers the codecs and relays a statement can accept

  REPL completion of a codec or relay reference applies the rules the visual creation forms apply
  to the same reference. A codec is offered where its clause uses it in a direction the codec
  supports, and a relay where it is compatible with the relays the statement has already named.
  The first input of a statement is compatible with every relay, because the clauses that would
  narrow it come after it. A correlator's first right input keeps the branch its left inputs read,
  and a generator's materialized relay needs materialized state on whichever branch follows it.

  Scenario Outline: Codec completion follows the direction its clause uses the codec in on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA reading ( id I64, value STRING );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( id integer, value string );
      CREATE WIRE JSON CODEC both_ways FROM WIRE SCHEMA reading_wire TO SCHEMA reading;
      CREATE JAQ JSON CODEC decode_only TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE JAQ JSON CODEC encode_only TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON EMITTING '.';
      """
    Then the current leader node is saved as placeholder "leader"
    And the CLI completion after each input on node "{{leader}}" offers exactly
      | input                                                                                                                              | offers                              |
      | CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING                         | both_ways, decode_only              |
      | ALTER HTTP ENDPOINT INGESTOR source SET DECODE USING                                                                               | both_ways, decode_only              |
      | CREATE HASH MAP lookup_by_id KEY id FROM RESOURCE bundle VERSION 1 PATH 'lookup.jsonl' DECODE USING                                | both_ways, decode_only              |
      | CREATE KAFKA TOPIC EMITTER sink FROM readings TO broker TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING | both_ways, encode_only              |
      | ALTER KAFKA TOPIC EMITTER sink SET ENCODE USING                                                                                    | both_ways, encode_only              |
      | DROP CODEC                                                                                                                         | both_ways, decode_only, encode_only |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Relay completion follows the relays the statement already names on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA reading ( id I64, value STRING );
      CREATE SCHEMA label ( id I64, name STRING );
      CREATE SCHEMA reading_key ( id I64 );
      CREATE BRANCH by_reading SCHEMA reading_key TTL 1h;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY more_readings SCHEMA reading UNBRANCHED;
      CREATE RELAY labels SCHEMA label UNBRANCHED;
      CREATE RELAY branched_readings SCHEMA reading BRANCHED BY by_reading;
      CREATE RELAY state_readings SCHEMA reading UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY branched_state SCHEMA reading BRANCHED BY by_reading WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      """
    Then the current leader node is saved as placeholder "leader"
    And the CLI completion after each input on node "{{leader}}" offers exactly
      | input                                                                                                                                                                                         | offers                                                                             |
      | CREATE JUNCTION copy FROM                                                                                                                                                                     | branched_readings, branched_state, labels, more_readings, readings, state_readings |
      | CREATE JUNCTION copy FROM readings,                                                                                                                                                           | more_readings, readings, state_readings                                            |
      | CREATE JUNCTION copy FROM readings UNBRANCHED USING MATERIALIZED STATE                                                                                                                        | state_readings                                                                     |
      | CREATE JUNCTION copy FROM readings UNBRANCHED TO                                                                                                                                              | labels, more_readings, readings, state_readings                                    |
      | CREATE JUNCTION copy FROM readings UNBRANCHED TO more_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR SEND TO                                                                           | labels, more_readings, readings, state_readings                                    |
      | CREATE CORRELATOR pairs LEFT FROM readings RIGHT FROM                                                                                                                                         | labels, more_readings, readings, state_readings                                    |
      | CREATE CORRELATOR pairs LEFT FROM branched_readings RIGHT FROM                                                                                                                                | branched_readings, branched_state                                                  |
      | CREATE CORRELATOR pairs LEFT FROM readings RIGHT FROM labels,                                                                                                                                 | labels                                                                             |
      | CREATE GENERATOR synth USING MATERIALIZED STATE                                                                                                                                               | branched_state, state_readings                                                     |
      | CREATE KAFKA TOPIC EMITTER sink FROM readings,                                                                                                                                                | branched_readings, branched_state, more_readings, readings, state_readings         |
      | CREATE KAFKA TOPIC EMITTER sink FROM readings USING MATERIALIZED STATE                                                                                                                        | state_readings                                                                     |
      | CREATE KAFKA TOPIC EMITTER sink FROM readings TO broker TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING codec INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR SEND TO | labels, more_readings, readings, state_readings                                    |
      | CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec TO readings INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR SEND TO  | labels, more_readings, readings, state_readings                                    |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

@nats_envelope_metadata @deloxide_stress
Feature: NATS message metadata and publication envelopes
  Scenario Outline: A NATS SUBJECT emitter refuses a plaintext server when the client requires TLS
    Given NATS is running
    And the plaintext NATS endpoint is addressed with the TLS scheme
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = '{{nats_tls_scheme_addr}}' };
      CREATE NATS SUBJECT EMITTER events_out FROM events
        TO nats_main SUBJECT 'tls_refusal_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "events_out" on the leader node contains
      """
      received corrupt message of type InvalidContentType
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: NATS STREAM metadata carries the server's delivery identity and event time
    Given NATS is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS JetStream stream "metadata_{{test_id}}" exists for subject "stream_metadata_{{test_id}}"
    And NATS JetStream durable pull consumer "reader_{{test_id}}" exists on stream "metadata_{{test_id}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA tagged ( seq I64, subject STRING OPTIONAL, reply STRING OPTIONAL,
        stream STRING OPTIONAL, consumer STRING OPTIONAL, sequence U64 OPTIONAL,
        timestamp DATETIME OPTIONAL, delivered U64 OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA tagged UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE NATS STREAM INGESTOR stream_events
        FROM nats_main STREAM 'metadata_{{test_id}}' CONSUMER 'reader_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec TIMESTAMP AT metadata.timestamp
        TO events INHERIT ALL SET subject = metadata.subject, reply = metadata.reply,
            stream = metadata.stream, consumer = metadata.consumer, sequence = metadata.sequence,
            timestamp = metadata.timestamp, delivered = metadata.delivered
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    When NATS JetStream message is published to subject "stream_metadata_{{test_id}}"
      """
      {"seq":7}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":7 | "subject":"stream_metadata_{{test_id}}" | "stream":"metadata_{{test_id}}" | "consumer":"reader_{{test_id}}" | "sequence":1 | "delivered":1 | "reply":"$JS.ACK. | "timestamp":"
      """
    And within "30s" NATS JetStream consumer "reader_{{test_id}}" of stream "metadata_{{test_id}}" acknowledges through stream sequence 1 with nothing pending

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: NATS wildcard messages project the concrete subject and optional reply
    Given NATS is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA tagged ( seq I64, subject STRING OPTIONAL, reply STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA tagged UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE NATS SUBJECT INGESTOR nats_events
        FROM nats_main SUBJECT 'metadata_{{test_id}}.*' QUEUE GROUP 'metadata_{{test_id}}'
        INSTANCES 1 MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL SET subject = metadata.subject, reply = coalesce(metadata.reply, 'no_reply')
        WHERE metadata.subject = 'metadata_{{test_id}}.created' OR metadata.subject = 'metadata_{{test_id}}.updated'
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "nats_events" on the leader node contains
      """
      ready: true
      """
    When NATS message is published to subject "metadata_{{test_id}}.created"
      """
      {"seq":7}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":7 | "subject":"metadata_{{test_id}}.created" | "reply":"no_reply"
      """
    When NATS message with reply "reply_{{test_id}}" is published to subject "metadata_{{test_id}}.updated"
      """
      {"seq":8}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":8 | "subject":"metadata_{{test_id}}.updated" | "reply":"reply_{{test_id}}"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A NATS SUBJECT emitter sends a request with its evaluated reply subject
    Given NATS is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "request_{{test_id}}" is observed
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY requests SCHEMA event UNBRANCHED;
      CREATE RELAY responses SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE NATS SUBJECT INGESTOR request_in
        FROM nats_main SUBJECT 'request_in_{{test_id}}' QUEUE GROUP 'request_in_{{test_id}}'
        INSTANCES 1 MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO requests INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER request_out FROM requests
        TO nats_main SUBJECT 'request_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        REPLY TO CASE WHEN input.seq = 1 THEN 'response_{{test_id}}' ELSE 'unused_{{test_id}}' END
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT INGESTOR response_in
        FROM nats_main SUBJECT 'response_{{test_id}}' QUEUE GROUP 'response_{{test_id}}'
        INSTANCES 1 MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO responses INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION response_subscription TO responses;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "request_in" on the leader node contains
      """
      ready: true
      """
    And within "30s" DESCRIBE INGESTOR "response_in" on the leader node contains
      """
      ready: true
      """
    When 1 sequential NATS messages are published to subject "request_in_{{test_id}}"
      """
      {"seq":{{sequence}}}
      """
    And the observed NATS request to reply subject "response_{{test_id}}" is answered
      """
      {"seq":42}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "seq":42
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW CREATE EMITTER request_out;
      """
    Then the last command output contains
      """
      REPLY TO CASE
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: NATS publication preserves interleaved repeated header names
    Given NATS is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "ordered_headers_out_{{test_id}}" is observed
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE NATS SUBJECT INGESTOR events_in
        FROM nats_main SUBJECT 'ordered_headers_in_{{test_id}}' QUEUE GROUP 'headers_{{test_id}}'
        INSTANCES 1 MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER events_out FROM events
        TO nats_main SUBJECT 'ordered_headers_out_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL INVOKE write_header('first', 'one'), write_header('second', 'two'), write_header('first', 'three')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "events_in" on the leader node contains
      """
      ready: true
      """
    When 1 sequential NATS messages are published to subject "ordered_headers_in_{{test_id}}"
      """
      {"seq":{{sequence}}}
      """
    Then within "30s" the observed broker receives exactly these messages
      | payload   | key | headers                          | group |
      | {"seq":1} |     | first=one;second=two;first=three |       |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

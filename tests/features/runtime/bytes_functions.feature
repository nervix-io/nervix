Feature: Typed bytes and native encoding functions
  Scenario Outline: Binary values survive conversion and hashing across the public data path
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA text_input (
        id STRING,
        raw STRING
      );
      CREATE SCHEMA binary_output (
        id STRING,
        raw STRING,
        binary BYTES,
        hexadecimal STRING,
        base64_text STRING,
        restored STRING,
        sha_hex STRING,
        fast_hash U64
      );
      CREATE WIRE JSON SCHEMA text_input_wire MODE STRICT (
        id string,
        raw string
      );
      CREATE CODEC text_input_codec
        FROM WIRE JSON SCHEMA text_input_wire
        TO SCHEMA text_input;
      CREATE RELAY text_inputs SCHEMA text_input UNBRANCHED;
      CREATE RELAY binary_outputs SCHEMA binary_output UNBRANCHED;
      CREATE VHOST edge bytes-functions-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/bytes' TYPE HTTP;
      CREATE INGESTOR text_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING text_input_codec
        TO text_inputs
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION binary_transform
        FROM text_inputs
        UNBRANCHED
        TO binary_outputs
          INHERIT ALL
          SET binary = hex_decode('00ff'),
              hexadecimal = hex_encode(output.binary),
              base64_text = base64_encode(output.binary),
              restored = bytes_to_utf8(base64_decode(base64_encode(bytes_from_utf8(input.raw)))),
              sha_hex = hex_encode(sha256(output.binary)),
              fast_hash = xxh3_64(output.binary)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION binary_outputs_subscription TO binary_outputs;
      START;
      """
    And http payload is posted to node "node-1" with host "bytes-functions-{{test_id}}.example.com" path "/bytes"
      """
      {"id":"binary","raw":"snowman ☃"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"binary" | "binary":"AP8=" | "hexadecimal":"00ff" | "base64_text":"AP8=" | "restored":"snowman ☃" | "sha_hex":"06eb7d6a69ee19e5fbdf749018d3d2abfa04bcbd1365db312eb86dc7169389b8"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Invalid binary text takes the structured message error route
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA encoded_input (id STRING, encoded STRING);
      CREATE SCHEMA binary_output (id STRING, prefix BYTES, payload BYTES);
      CREATE SCHEMA binary_error (id STRING, error_code STRING, operation STRING, captured_prefix BYTES OPTIONAL);
      CREATE WIRE JSON SCHEMA encoded_wire MODE STRICT (id string, encoded string);
      CREATE CODEC encoded_codec FROM WIRE JSON SCHEMA encoded_wire TO SCHEMA encoded_input;
      CREATE RELAY encoded_inputs SCHEMA encoded_input UNBRANCHED;
      CREATE RELAY binary_outputs SCHEMA binary_output UNBRANCHED;
      CREATE RELAY binary_errors SCHEMA binary_error UNBRANCHED;
      CREATE VHOST edge bytes-error-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/bytes' TYPE HTTP;
      CREATE INGESTOR encoded_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING encoded_codec
        TO encoded_inputs
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION binary_transform
        FROM encoded_inputs
        UNBRANCHED
        TO binary_outputs
          SET id = input.id,
              prefix = hex_decode('00ff'),
              payload = hex_decode(input.encoded)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO binary_errors
            SET id = input.id,
                error_code = error.code,
                operation = error.operation,
                captured_prefix = partial_output.prefix;
      CREATE SUBSCRIPTION binary_errors_subscription TO binary_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "bytes-error-{{test_id}}.example.com" path "/bytes"
      """
      {"id":"malformed","encoded":"00zz"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"malformed" | "error_code":"evaluation" | "operation":"set" | "captured_prefix":"AP8="
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Schemaful JSON bytes decode without text coercion
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA binary_input (id STRING, payload BYTES);
      CREATE SCHEMA binary_text (id STRING, hexadecimal STRING);
      CREATE WIRE JSON SCHEMA binary_wire MODE STRICT (id STRING, payload BYTES);
      CREATE CODEC binary_codec FROM WIRE JSON SCHEMA binary_wire TO SCHEMA binary_input;
      CREATE RELAY binary_inputs SCHEMA binary_input UNBRANCHED;
      CREATE RELAY binary_texts SCHEMA binary_text UNBRANCHED;
      CREATE VHOST edge bytes-wire-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/bytes' TYPE HTTP;
      CREATE INGESTOR binary_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING binary_codec
        TO binary_inputs
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION binary_transform
        FROM binary_inputs
        UNBRANCHED
        TO binary_texts
          INHERIT id
          SET hexadecimal = hex_encode(input.payload)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION binary_texts_subscription TO binary_texts;
      START;
      """
    And http payload is posted to node "node-1" with host "bytes-wire-{{test_id}}.example.com" path "/bytes"
      """
      {"id":"binary","payload":"AP8="}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"binary" | "hexadecimal":"00ff"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Encoding and hashing sensitive bytes keeps subscription values masked
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA binary_input (id STRING, secret BYTES SENSITIVE);
      CREATE SCHEMA binary_output (id STRING, encoded STRING SENSITIVE, digest BYTES SENSITIVE, fast_hash U64 SENSITIVE);
      CREATE WIRE JSON SCHEMA binary_wire MODE STRICT (id STRING, secret BYTES);
      CREATE CODEC binary_codec FROM WIRE JSON SCHEMA binary_wire TO SCHEMA binary_input;
      CREATE RELAY binary_inputs SCHEMA binary_input UNBRANCHED;
      CREATE RELAY binary_outputs SCHEMA binary_output UNBRANCHED;
      CREATE VHOST edge bytes-sensitive-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/bytes' TYPE HTTP;
      CREATE INGESTOR binary_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING binary_codec
        TO binary_inputs
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION binary_transform
        FROM binary_inputs
        UNBRANCHED
        TO binary_outputs
          INHERIT id
          SET encoded = base64_encode(input.secret),
              digest = sha256(input.secret),
              fast_hash = xxh3_64(input.secret)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION binary_outputs_subscription TO binary_outputs;
      START;
      """
    And http payload is posted to node "node-1" with host "bytes-sensitive-{{test_id}}.example.com" path "/bytes"
      """
      {"id":"sensitive","secret":"AP8="}
      """
    Then the relay subscription receives a payload
      """
      "id":"sensitive"
      """
    And the last relay subscription payload masks field "encoded"
    And the last relay subscription payload masks field "digest"
    And the last relay subscription payload masks field "fast_hash"
    And the last relay subscription payload does not contain "AP8="

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

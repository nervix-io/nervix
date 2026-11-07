@kinded_codec_heads
Feature: Codec declaration heads select their family and format

  Scenario Outline: A wire codec head selects its exact schema format
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA payload (value STRING);
      CREATE WIRE JSON SCHEMA json_wire MODE STRICT (value string);
      CREATE WIRE CBOR SCHEMA cbor_wire MODE STRICT (value text);
      CREATE WIRE AVRO SCHEMA avro_wire MODE STRICT (value string);
      CREATE WIRE <format> CODEC payload_codec FROM WIRE SCHEMA <wire_schema> TO SCHEMA payload;
      """
    And these Plexus commands are executed on the active session
      """
      SHOW CREATE CODEC payload_codec;
      """
    Then the last command output contains
      """
      CREATE WIRE <format> CODEC payload_codec
        FROM WIRE SCHEMA <wire_schema>
        TO SCHEMA payload;
      """
    And the current leader node is saved as placeholder "leader"
    When the CLI suggests for "CREATE WIRE <format> CODEC next_codec FROM WIRE SCHEMA |" on node "{{leader}}"
    Then the CLI offers exactly "<wire_schema>"

    Examples:
      | cluster_size | format | wire_schema |
      | 1            | JSON   | json_wire   |
      | 3            | JSON   | json_wire   |
      | 1            | CBOR   | cbor_wire   |
      | 3            | CBOR   | cbor_wire   |
      | 1            | AVRO   | avro_wire   |
      | 3            | AVRO   | avro_wire   |

  Scenario Outline: A JAQ codec head preserves its format and directed transformations
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA payload (value STRING);
      CREATE JAQ <format> CODEC payload_codec TO SCHEMA payload
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '.';
      """
    And these Plexus commands are executed on the active session
      """
      SHOW CREATE CODEC payload_codec;
      """
    Then the last command output contains
      """
      CREATE JAQ <format> CODEC payload_codec
        TO SCHEMA payload
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '.';
      """

    Examples:
      | cluster_size | format |
      | 1            | JSON   |
      | 3            | JSON   |
      | 1            | YAML   |
      | 3            | YAML   |
      | 1            | TOML   |
      | 3            | TOML   |
      | 1            | XML    |
      | 3            | XML    |
      | 1            | CBOR   |
      | 3            | CBOR   |

  Scenario Outline: A Syslog codec head uses its predefined wire contract
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA syslog_event (
        facility U8, severity U8, timestamp DATETIME OPTIONAL,
        hostname STRING OPTIONAL, app_name STRING OPTIONAL, proc_id STRING OPTIONAL,
        msg_id STRING OPTIONAL, structured_data STRING OPTIONAL, message STRING
      );
      CREATE SYSLOG CODEC payload_codec TO SCHEMA syslog_event;
      """
    And these Plexus commands are executed on the active session
      """
      SHOW CREATE CODEC payload_codec;
      """
    Then the last command output contains
      """
      CREATE SYSLOG CODEC payload_codec
        TO SCHEMA syslog_event;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Protobuf codec head keeps its exact resource binding
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "proto_dir" containing
      """
      {"payload.proto":"syntax = \"proto3\";\npackage test;\nmessage Payload { string value = 1; }\n"}
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE RESOURCE bundle;
      UPLOAD RESOURCE bundle VERSION '{{proto_dir}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA payload (value STRING);
      CREATE PROTOBUF CODEC payload_codec USING RESOURCE bundle VERSION 1
        CONFIG {'file' = 'payload.proto'} MESSAGE 'test.Payload'
        TO SCHEMA payload WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '.';
      """
    And these Plexus commands are executed on the active session
      """
      SHOW CREATE CODEC payload_codec;
      """
    Then the last command output contains
      """
      CREATE PROTOBUF CODEC payload_codec
      """
    And the last command output contains
      """
      USING RESOURCE bundle VERSION 1
      """
    And the last command output contains
      """
      MESSAGE 'test.Payload'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Session completion offers codec family heads as whole phrases
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "CREATE |" on node "{{leader}}"
    Then the CLI suggestion "WIRE JSON CODEC" applied to "CREATE |" yields "CREATE WIRE JSON CODEC"
    And the CLI suggestion "WIRE CBOR CODEC" applied to "CREATE |" yields "CREATE WIRE CBOR CODEC"
    And the CLI suggestion "WIRE AVRO CODEC" applied to "CREATE |" yields "CREATE WIRE AVRO CODEC"
    And the CLI suggestion "JAQ JSON CODEC" applied to "CREATE |" yields "CREATE JAQ JSON CODEC"
    And the CLI suggestion "JAQ YAML CODEC" applied to "CREATE |" yields "CREATE JAQ YAML CODEC"
    And the CLI suggestion "JAQ TOML CODEC" applied to "CREATE |" yields "CREATE JAQ TOML CODEC"
    And the CLI suggestion "JAQ XML CODEC" applied to "CREATE |" yields "CREATE JAQ XML CODEC"
    And the CLI suggestion "JAQ CBOR CODEC" applied to "CREATE |" yields "CREATE JAQ CBOR CODEC"
    And the CLI suggestion "PROTOBUF CODEC" applied to "CREATE |" yields "CREATE PROTOBUF CODEC"
    And the CLI suggestion "SYSLOG CODEC" applied to "CREATE |" yields "CREATE SYSLOG CODEC"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

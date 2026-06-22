Feature: Ingestor filter-map logic
  Scenario Outline: Ingestor filter-map rewrites and filters records for supported transports
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "<transport_fixture>" starts with output schema "rewritten" and program
      """
      SET logic_notifications.amount = message.amount + 1, logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw WHERE message.active
      """
    And the ingestor logic transport "<transport_fixture>" delivers payload fixture "mixed_filter_messages"
    Then the ingestor logic expectation "rewritten_filtered_once" is observed

    Examples:
      | cluster_size | transport_fixture  |
      | 1            | http_endpoint      |
      | 3            | http_endpoint      |
      | 1            | websocket_endpoint |
      | 3            | websocket_endpoint |
      | 1            | zeromq             |
      | 3            | zeromq             |

  Scenario Outline: Ingestor filter-map can route protocol headers explicitly
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "<transport_fixture>" starts with output schema "header_routed" and program
      """
      SET logic_notifications.amount = message.amount + 1, logic_notifications.normalized = headers.route UNSET logic_notifications.raw WHERE headers.tenant = message.tenant
      """
    And the ingestor logic transport "<transport_fixture>" delivers payload fixture "header_message" with headers
    Then the ingestor logic expectation "header_routed_once" is observed

    Examples:
      | cluster_size | transport_fixture |
      | 1            | http_endpoint     |
      | 3            | http_endpoint     |
      | 1            | kafka             |
      | 3            | kafka             |
      | 1            | nats              |
      | 3            | nats              |

  Scenario Outline: Ingestor filter-map leader validation rejects invalid programs
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "<transport_fixture>" fails to start with output schema "<output_schema_fixture>" and program
      """
      <logic_program>
      """
    Then the ingestor logic expectation "compile_error" is observed

    Examples:
      | cluster_size | transport_fixture  | output_schema_fixture | logic_program                                                                         |
      | 1            | http_endpoint      | input                 | SET logic_notifications.total = message.missing + 1                                   |
      | 3            | http_endpoint      | input                 | SET logic_notifications.total = message.missing + 1                                   |
      | 1            | kafka              | input                 | SET logic_notifications.total = message.missing + 1                                   |
      | 3            | kafka              | input                 | SET logic_notifications.total = message.missing + 1                                   |
      | 1            | websocket_endpoint | input                 | SET logic_notifications.total = message.missing + 1                                   |
      | 3            | websocket_endpoint | input                 | SET logic_notifications.total = message.missing + 1                                   |
      | 1            | zeromq             | input                 | SET logic_notifications.total = message.missing + 1                                   |
      | 3            | zeromq             | input                 | SET logic_notifications.total = message.missing + 1                                   |
      | 1            | http_endpoint      | input                 | SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw |
      | 3            | http_endpoint      | input                 | SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw |
      | 1            | kafka              | input                 | SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw |
      | 3            | kafka              | input                 | SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw |
      | 1            | websocket_endpoint | input                 | SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw |
      | 3            | websocket_endpoint | input                 | SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw |
      | 1            | zeromq             | input                 | SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw |
      | 3            | zeromq             | input                 | SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw |
      | 1            | http_endpoint      | input                 | SET logic_notifications.total = metadata.offset                                       |
      | 3            | http_endpoint      | input                 | SET logic_notifications.total = metadata.offset                                       |
      | 1            | http_endpoint      | input                 | SET message.amount = message.amount + 1                                               |
      | 3            | http_endpoint      | input                 | SET message.amount = message.amount + 1                                               |

  Scenario Outline: Ingestor filter-map runtime failures emit errors and drop messages
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "<transport_fixture>" starts with output schema "parsed" and program
      """
      SET logic_notifications.parsed = message.raw AS INT64 UNSET logic_notifications.active, logic_notifications.amount, logic_notifications.raw
      """
    And the ingestor logic transport "<transport_fixture>" delivers payload fixture "runtime_failure_message"
    Then the ingestor logic expectation "runtime_error_drop" is observed

    Examples:
      | cluster_size | transport_fixture  |
      | 1            | http_endpoint      |
      | 3            | http_endpoint      |
      | 1            | websocket_endpoint |
      | 3            | websocket_endpoint |
      | 1            | zeromq             |
      | 3            | zeromq             |

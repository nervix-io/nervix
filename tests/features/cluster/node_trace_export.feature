Feature: A node's own trace export follows its declared options alone
  A node exports its own traces once its collector endpoint is provisioned, through the
  --otel-otlp-endpoint option or its environment twin, and the option outranks the twin. The
  node's trace export capabilities decide how it exports, and DESCRIBE CLUSTER shows them. No
  OpenTelemetry environment variable changes where a node sends its traces, what it sends or
  whether it starts.

  Scenario Outline: No OpenTelemetry environment variable changes a node's trace export on a cluster of <cluster_size>
    Given OTLP receiver "node traces" is running for "grpc"
    And OTLP receiver "twin traces" is running for "grpc"
    And OTLP receiver "environment traces" is running for "grpc"
    When a <cluster_size> node server process cluster starts with its own trace export to OTLP receiver "node traces", the option's environment twin naming OTLP receiver "twin traces" and every OpenTelemetry environment variable naming OTLP receiver "environment traces"
    Then OTLP receiver "node traces" eventually receives the server's own spans from every node
    And OTLP receiver "node traces" received nothing the OpenTelemetry environment asked for
    And OTLP receiver "twin traces" has captured exactly 0 export requests
    And OTLP receiver "environment traces" has captured exactly 0 export requests
    When every trace-exporting node receives SIGTERM
    Then every trace-exporting node exits successfully

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The option's environment twin alone provisions the collector on a cluster of <cluster_size>
    Given OTLP receiver "twin traces" is running for "grpc"
    When a <cluster_size> node server process cluster starts with its own trace export to OTLP receiver "twin traces" named only by the option's environment twin
    Then OTLP receiver "twin traces" eventually receives the server's own spans from every node
    When every trace-exporting node receives SIGTERM
    Then every trace-exporting node exits successfully

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Configured trace export capabilities are visible through inspection on a cluster of <cluster_size>
    Given node-1 starts with otel-trace-sample-ratio = 0.25
    And node-1 starts with otel-trace-export-timeout = 7s
    And node-1 starts with otel-trace-export-interval = 2s
    And node-1 starts with otel-trace-queue-capacity = 4096
    And node-1 starts with otel-trace-export-batch-spans = 256
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed through the client on node "node-1"
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      otel-trace-sample-ratio=0.25 otel-trace-export-timeout=7s otel-trace-export-interval=2s otel-trace-queue-capacity=4096 otel-trace-export-batch-spans=256
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

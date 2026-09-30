Feature: The node's own trace export uses its configured name resolver

  Scenario Outline: Every node exports its own startup traces through fixture DNS
    Given OTLP receiver "node traces" is running for "grpc"
    When a <cluster_size> node server process cluster starts with its own trace export to OTLP receiver "node traces" through fixture DNS
    Then OTLP receiver "node traces" eventually receives the server's own spans from every node
    When every trace-exporting node receives SIGTERM
    Then every trace-exporting node exits successfully

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

Feature: Cluster observability

  Scenario: Trace inspection dependencies start on demand
    Given Jaeger is running
    Then dependency endpoint "quickwit_addr" responds with 200
    And dependency endpoint "jaeger_addr" responds with 200

  Scenario: Failure diagnostics redact dependency secrets
    Given Redis is running
    Then failure diagnostics redact dependency certificate bytes and endpoint values

  Scenario: Ephemeral dependencies are removed when a test suite unwinds
    Then an ephemeral dependency is removed when its test suite unwinds

  Scenario: Kafka exposes host and Docker network benchmark endpoints
    Given Kafka is running
    Then Kafka exposes host and Docker network benchmark endpoints

  Scenario Outline: On-demand dependency is shared for the test suite attempt <attempt>
    Given Quickwit is running
    Then dependency endpoint "quickwit_addr" remains stable for the test suite
    And dependency endpoint "quickwit_addr" responds with 200

    Examples:
      | attempt |
      | 1       |
      | 2       |
      | 3       |

  Scenario Outline: Observability endpoints report node health and allocator metrics
    Given a <cluster_size> node nervix cluster is started
    Then node "<node_id>" observability path "/livez" eventually responds with 200 and "live"
    And node "<node_id>" observability path "/readyz" eventually responds with 200 and "ready"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_jemalloc_active_bytes"

    Examples:
      | cluster_size | node_id |
      | 1            | node-1  |
      | 3            | node-1  |
      | 3            | node-2  |
      | 3            | node-3  |

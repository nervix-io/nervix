Feature: Client wire performance baseline
  @client_wire_baseline @exclusive
  Scenario: Capture the current native gRPC and console WebSocket protocol costs
    Given a release nervix-server process is started for the client-wire baseline
    When the current client-wire baseline is captured
    Then the client-wire baseline artifact exists

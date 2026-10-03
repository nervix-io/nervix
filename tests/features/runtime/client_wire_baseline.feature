Feature: Client wire performance baseline
  @client_wire_baseline
  Scenario: Capture native gRPC and WebSocket costs with paused subscriber control traffic
    Given a release nervix-server process is started for the client-wire baseline
    When the current client-wire baseline is captured
    Then the client-wire baseline artifact exists

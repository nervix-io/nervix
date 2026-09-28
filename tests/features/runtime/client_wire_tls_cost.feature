@client_wire_tls_cost
Feature: Client wire command transport cost
  Scenario Outline: The same command is timed over <mode> gRPC
    Given client grpc transport is configured with mode "<mode>"
    And a 1 node nervix cluster is started
    And the active domain is "wire_tls"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN wire_tls;
      """
    And the client-wire command transport cost is captured
    Then the client-wire command transport artifact exists

    Examples:
      | mode  |
      | http  |
      | https |

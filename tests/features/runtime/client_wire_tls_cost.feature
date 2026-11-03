@client_wire_tls_cost
Feature: Client wire command transport cost
  Scenario: The same command and fresh connections are timed in pairs over plaintext and TLS gRPC
    Given plaintext and TLS release nervix-server processes are started for the client-wire transport cost
    When the paired client-wire transport cost is captured
    Then the client-wire command transport artifact exists

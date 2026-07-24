Feature: Server executable identity

  Scenario: Server help identifies the dedicated server executable
    When the nervix-server help is requested
    Then the last command output contains
      """
      Usage: nervix-server [OPTIONS]
      """
    And the legacy nervix server executable is absent

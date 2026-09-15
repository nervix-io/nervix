Feature: Process termination signals

  Scenario Outline: The first termination signal starts graceful shutdown of a server process
    Given a nervix-server process is started
    When the server process receives <signal>
    Then the server process exits with status 0
    And the server process log contains "termination signal received; requesting graceful shutdown signal=<signal>"
    And the server process log contains "shutdown terminal-teardown phase finished"

    Examples:
      | signal  |
      | SIGINT  |
      | SIGTERM |

  Scenario Outline: A repeated termination signal forces termination while graceful shutdown is stalled
    Given a nervix-server process is started
    And an authenticated resource upload is held open on the server process
    When the server process receives <first>
    Then the server process log eventually contains "termination signal received; requesting graceful shutdown signal=<first>"
    When the server process receives <second>
    Then the server process exits with status <status> within "10s" of the last signal
    And the server process log contains "repeated termination signal received; abandoning graceful shutdown signal=<second>"
    And the server process log does not contain "shutdown terminal-teardown phase finished"

    Examples:
      | first   | second  | status |
      | SIGINT  | SIGINT  | 130    |
      | SIGTERM | SIGTERM | 143    |
      | SIGINT  | SIGTERM | 143    |
      | SIGTERM | SIGINT  | 130    |

  Scenario: A server process that cannot register termination signal handlers refuses to start
    Given a nervix-server process is started with an open file limit of 4
    Then the server process exits with status 1
    And the server process log contains "failed to register termination signal handlers"

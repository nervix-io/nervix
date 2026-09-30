Feature: Server command-line values

  Scenario Outline: A node refuses a duration option it cannot read and says why
    When a nervix-server process is started with command-line option "<option>" set to "<value>"
    Then the server process exits with status 2
    And the server process log contains "invalid value '<value>' for '<option>"
    And the server process log contains "invalid duration: <reason>"
    And the server process log does not contain "panicked"

    Examples:
      | option                    | value                              | reason                              |
      | --drain-timeout           | 18446744073709551615s 1000000000ns | it is longer than a duration can be |
      | --raft-heartbeat-interval | 18446744073709551615.5s500000000ns | it is longer than a duration can be |
      | --shutdown-timeout        | oops                               | expected number at 0                |

  Scenario: A node refuses a duration setting from its environment that it cannot read
    When a nervix-server process is started with environment variable "NERVIX_TRANSACTION_IDLE_TIMEOUT" set to "18446744073709551615s 1000000000ns"
    Then the server process exits with status 2
    And the server process log contains "invalid value '18446744073709551615s 1000000000ns' for '--transaction-idle-timeout"
    And the server process log contains "invalid duration: it is longer than a duration can be"
    And the server process log does not contain "panicked"

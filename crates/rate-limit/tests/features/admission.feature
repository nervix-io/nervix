Feature: Unit-owned whole-payload rate admission
  The engine's public API returns waits as data and restores allowance debt from injected clocks.

  Scenario Outline: Independent units refund refused payloads and wait for their latest clause
    Given <units> independent execution-unit limiters with 20 and 40 millisecond clauses
    When each unit admits its first whole payload
    And each unit checks another payload at its captured first-admission instant
    Then every refusal preserves the complete allowance generation
    And each unit admits its next payload at least 40 milliseconds after its first

    Examples:
      | units |
      | 1     |
      | 3     |

  Scenario Outline: A restored unit credits elapsed UTC exactly once
    Given <units> independent execution-unit limiters with 20 and 40 millisecond clauses
    When each unit admits its first whole payload
    And its generation is restored after 10 milliseconds of injected UTC
    Then each restored unit still owes exactly 30 milliseconds before the next whole payload

    Examples:
      | units |
      | 1     |
      | 3     |

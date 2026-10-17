Feature: Postgres observation fixtures
  The shared database serves concurrent scenario observers without allocating a separate
  connection pool for every setup or observation. Verification uses the same SQLx driver and
  TLS policy as ordinary Postgres steps.

  Scenario Outline: Concurrent Postgres fixture observers retain usable clients over <transport>
    Given Postgres is running
    When 48 concurrent Postgres fixture observers retain 3 clients over <transport>
    Then every Postgres fixture observer received its complete query results

    Examples:
      | transport |
      | plaintext |
      | TLS       |

  Scenario Outline: Postgres observers keep capacity while two other fixtures hold locks over <transport>
    Given Postgres is running
    And two other Postgres fixtures hold table locks over <transport>
    When another Postgres fixture observer queries over <transport>
    Then every Postgres fixture observer received its complete query results

    Examples:
      | transport |
      | plaintext |
      | TLS       |

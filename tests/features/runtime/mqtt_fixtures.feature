Feature: MQTT fixture publication
  Scenario Outline: Publication waits for a delayed connection to send its payload
    Given MQTT is running
    When <count> QoS <qos> MQTT fixture payloads are published through a "500ms" connection stall
      """
      {"marker":"{{test_id}}","number":42,"bytes":"00ff","items":[1,2,3]}
      """
    Then every MQTT fixture payload reaches its observer unchanged

    Examples:
      | count | qos |
      | 1     | 0   |
      | 1     | 1   |
      | 5     | 0   |
      | 5     | 1   |

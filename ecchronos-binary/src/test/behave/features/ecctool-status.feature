Feature: ecctool status

  Scenario: Get status with json output option
    Given we have access to ecctool
    When we list status with json output option
    Then the output should contain valid json data
    And the json output should contain nodes

  Scenario: Get status with table output
    Given we have access to ecctool
    When we list status
    Then the output should contain a valid state nodes header

  Scenario: Get local status with legacy health check
    Given we have access to ecctool
    When we list local status
    Then the output should contain local ecchronos running status

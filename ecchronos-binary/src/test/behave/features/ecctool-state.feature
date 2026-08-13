Feature: ecctool state

  Scenario: Get state nodes with json output option
    Given we have access to ecctool
    When we list all state nodes with json output option
    Then the output should contain valid json data
    And the json output should contain nodes

  Scenario: Get state nodes with table output
    Given we have access to ecctool
    When we list all state nodes
    Then the output should contain a valid state nodes header

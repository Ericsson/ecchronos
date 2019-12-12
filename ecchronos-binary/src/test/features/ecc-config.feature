Feature: ecc-config

  Scenario: List config
    Given we have access to ecc-config
    When we list config
    Then the config output should contain a valid header
    And the config output should contain a row for test.table1
    And the config output should contain a row for test.table2
    And the config output should contain a row for test2.table1
    And the config output should contain a row for test2.table2
    And the config output should not contain more rows

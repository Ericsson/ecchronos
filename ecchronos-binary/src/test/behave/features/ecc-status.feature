Feature: ecc-status

  Scenario: List tables
    Given we have access to ecc-status
    And we schedule an on demand repair on test2.table2
    When we list all tables
    Then the output should contain a valid header
    And the output should contain a row for test.table1
    And the output should contain a row for test.table2
    And the output should contain a row for test2.table1
    And the output should contain a row for test2.table2
    And the output should contain a row for test2.table2
    And the output should contain a row for test2.table2
    And the output should not contain more rows
    And the output should contain summary

  Scenario: List tables with a limit
    Given we have access to ecc-status
    When we list all tables with a limit of 1
    Then the output should contain a valid header
    And the output should contain 1 row
    And the output should contain summary

  Scenario: List tables for keyspace test
    Given we have access to ecc-status
    When we list all tables for keyspace test
    Then the output should contain a valid header
    And the output should contain a row for test.table1
    And the output should contain a row for test.table2
    And the output should not contain more rows
    And the output should contain summary

  Scenario: List tables for keyspace test with a limit
    Given we have access to ecc-status
    When we list all tables for keyspace test with a limit of 1
    Then the output should contain a valid header
    And the output should contain a row for test..*
    And the output should not contain more rows
    And the output should contain summary

  Scenario: Show the table test.table1
    Given we have access to ecc-status
    When we list jobs for table test.table1
    Then the output should contain a valid header
    And the output should contain a row for test.table1
    And the output should not contain more rows
    And the output should contain summary

  Scenario: Show the table test.table2 with a limit
    Given we have access to ecc-status
    When we show job test.table2 with a limit of 5
    Then the expected header should be for test.table2
    And the token list should contain 5 rows

  Scenario: Show the table test.table1 with a limit
    Given we have access to ecc-status
    When we show job test.table2 with a limit of 15
    Then the expected header should be for test.table2
    And the token list should contain 15 rows
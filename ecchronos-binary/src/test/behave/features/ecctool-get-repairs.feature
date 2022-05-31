Feature: ecctool repairs

  Scenario: List repairs
    Given we have access to ecctool
    And we schedule an on demand repair on test2.table1
    When we list all repairs
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2
    And the output should contain a repair row for test2.table1
    And the output should contain a repair row for test2.table1
    And the output should not contain more rows
    And the output should contain summary

  Scenario: List repairs with a limit
    Given we have access to ecctool
    When we list all repairs with limit of 1
    Then the output should contain a valid repair header
    And the output should contain 1 repair rows
    And the output should not contain more rows
    And the output should contain summary

  Scenario: List repairs for keyspace test2
    Given we have access to ecctool
    When we list all repairs for keyspace test2
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2
    And the output should contain a repair row for test2.table1
    And the output should contain a repair row for test2.table1
    And the output should not contain more rows
    And the output should contain summary

  Scenario: List repairs for keyspace test2 with a limit
    Given we have access to ecctool
    When we list all repairs for keyspace test2 with a limit of 1
    Then the output should contain a valid repair header
    And the repair output should contain a valid repair row for test2..*
    And the output should not contain more rows
    And the output should contain summary

  Scenario: List the repair test2.table2
    Given we have access to ecctool
    When we list repairs for table test2.table2
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2
    And the output should not contain more rows
    And the output should contain summary

  Scenario: List the repairs test2.table2 with a limit
    Given we have access to ecctool
    When we list repairs test2.table2 with a limit of 15
    Then the output should contain a repair row for test2.table2
    And the output should not contain more rows
    And the output should contain summary
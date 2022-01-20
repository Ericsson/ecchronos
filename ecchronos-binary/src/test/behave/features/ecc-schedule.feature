Feature: ecc-schedule

  Scenario: Schedule repair for keyspace test2 and table table2
    Given we have access to ecc-schedule
    When we schedule repair for keyspace test2 and table table2
    Then the schedule output should contain a valid header
    And the schedule output should contain a row for test2.table2
    And the schedule output should not contain more rows

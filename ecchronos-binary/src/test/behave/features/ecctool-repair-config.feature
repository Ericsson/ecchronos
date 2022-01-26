Feature: ecctool repair-config

  Scenario: List config
    Given we have access to ecctool
    When we list config
    Then the config output should contain a valid header
    And the config output should contain a row for test.table1
    And the config output should contain a row for test.table2
    And the config output should contain a row for test2.table1
    And the config output should contain a row for test2.table2
    And the config output should not contain more rows

  Scenario: List config for keyspace test
    Given we have access to ecctool
    When we list config for keyspace test
    Then the config output should contain a valid header
    And the config output should contain a row for test.table1
    And the config output should contain a row for test.table2
    And the config output should not contain more rows

  Scenario: List config for keyspace test and table table2
    Given we have access to ecctool
    When we list config for keyspace test and table table2
    Then the config output should contain a valid header
    And the config output should contain a row for test.table2
    And the config output should not contain more rows

  Scenario: Get defined config for table test.table1
    Given we have access to ecctool
    When we list config for keyspace test and table table1
    Then the config output should contain a valid header
    And the config output should contain a row for test.table1
    And the repair interval is 1 day(s) 00h 00m 00s
    And the unwind ratio is 0.1
    And the warning time is 4 day(s) 00h 00m 00s
    And the error time is 8 day(s) 00h 00m 00s
    And the config output should not contain more rows

  Scenario: Get specific config
    Given we have access to ecctool
    When we list a specific config for keyspace test and table table1
    Then the config output should contain a valid header
    And the config output should contain a row for test.table1
    And the repair interval is 1 day(s) 00h 00m 00s
    And the unwind ratio is 0.1
    And the warning time is 4 day(s) 00h 00m 00s
    And the error time is 8 day(s) 00h 00m 00s
    And the config output should not contain more rows


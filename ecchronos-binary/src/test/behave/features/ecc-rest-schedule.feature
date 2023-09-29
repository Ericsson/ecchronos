Feature: API to get schedule status

  Scenario: Get schedule status for all repairs
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/v2/schedules
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules

  Scenario: Get schedule status for all repairs in the keyspace test
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/v2/schedules?keyspace=test
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the job list contains only keyspace test

  Scenario: Get schedule status for table test.table1 and then get by id
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/v2/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the id from response is extracted for test.table1
    Given I have a json schema schedule
    And I fetch schedules with id
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedule

  Scenario: Get full schedule status for table test.table1 and then get by id
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/v2/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the id from response is extracted for test.table1
    Given I have a json schema full_schedule
    And I fetch schedules with id and full
    When I send a GET request
    Then the response is successful
    And the response matches the json schema full_schedule
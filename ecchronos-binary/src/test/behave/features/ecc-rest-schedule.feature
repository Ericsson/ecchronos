Feature: API to get schedule status


  Scenario: Get schedule status for table test.table1 v2
    Given I have a json schema in schedule_job_list_v2.json
    And I use the url http://localhost:8080/repair-management/v2/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schedule_job_list_v2

  Scenario: Get schedule status for all repairs
    Given I have a json schema in schedule_job_list_v2.json
    And I use the url http://localhost:8080/repair-management/v2/schedules
    When I send a GET request
    Then the response is successful
    And the response matches the json schedule_job_list_v2

  Scenario: Get schedule status for all repairs in the keyspace test
    Given I have a json schema in schedule_job_list_v2.json
    And I use the url http://localhost:8080/repair-management/v2/schedules?keyspace=test
    When I send a GET request
    Then the response is successful
    And the response matches the json schedule_job_list_v2
    And the job list contains only keyspace test
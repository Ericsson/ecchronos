Feature: API to get repair status of a single table

  Scenario: Show scheduled job for table test.table1
    Given I have a json schema in repair_job.json
    And I use the url http://localhost:8080/repair-scheduler/v1/get/test/table1
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_job

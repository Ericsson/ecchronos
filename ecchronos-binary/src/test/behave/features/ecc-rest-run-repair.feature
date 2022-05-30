Feature: API to schedule repairs

  Scenario: Schedule repair for table test2.table1
    Given I have a json schema in demand_repair_job_v2.json
    And I use the url http://localhost:8080/repair-management/v2/repairs?keyspace=test2&table=table1
    When I send a POST request
    Then the response is successful
    And the response matches the json demand_repair_job_v2
    And the job for test2.table1 change status to completed
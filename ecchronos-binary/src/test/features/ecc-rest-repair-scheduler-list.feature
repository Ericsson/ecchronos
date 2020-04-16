Feature: API to get repair status of multiple tables

  Background: Schema setup
    Given I have a json schema in repair_job_list.json

  Scenario: List all scheduled repairs
    Given I use the url http://localhost:8080/repair/scheduled/v1/list
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_job_list

  Scenario: List all scheduled repairs for keyspace test
    Given I use the url http://localhost:8080/repair/scheduled/v1/list/test
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_job_list
    And the job list contains only keyspace test

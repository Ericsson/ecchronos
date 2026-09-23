Feature: ecctool repair-sessions

  Scenario: List repair sessions
    Given we have access to ecctool
    When we list all repair sessions
    Then the output should contain a valid repair sessions header

  Scenario: List repair sessions as json
    Given we have access to ecctool
    When we list all repair sessions as json
    Then the repair sessions json output contains a repairSessions key

  Scenario: Fail a non-existent repair session
    Given we have access to ecctool
    When we fail repair session non-existent-session with yes
    Then the repair sessions command fails

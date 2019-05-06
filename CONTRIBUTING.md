# Contributing

You are most welcome to create pull requests and issues to ecChronos.
Before creating pull requests it is recommended to first discuss your idea with at least one of the owners of the repository.
For keeping track of the history it is recommended that most communication is performed or summarized within pull requests or issues.

## Branches

Target the minimum possible branch version when fixing an issue.
New features should in general be added to the master branch.

## Pull requests

1. Ensure that the pull request is targeted at the minimum possible version based on the type of change.
2. If you intend to fix an issue,
 mention the issue number in the text body of the pull requests, i.e. `Fixes #20`.
3. Add a changelog entry at the bottom of the current target version when handling an issue.
4. All checks must pass before merging a pull request.
5. In general at least one project admin should approve the pull request before merging.

## Merging

Merging is performed from minimum version towards master.
It is the responsibility of the person merging the pull request to make sure it gets merged to master.

## Code Style

Codestyle TBD.
It is appreciated if provided patches are contained and do not modify code outside of the scope of the patch.
This will also make it quicker to perform reviews and merging the pull requests.

## Builds & Tests

If you're looking to contribute to ecChronos it is recommended that you sign up on [Travis](https://travis-ci.org/) and [Coveralls](https://coveralls.io/) and enable builds and reports on your own fork of ecChronos.
The builds required to merge a pull requests are contained within the [Travis configuration](.travis.yml) and include tests, code coverage as well as PMD checks.

### Tests

Make sure to add tests to the relevant phase(s) when possible.
If mocks or a single Apache Cassandra instance is necessary it may be run as a unit test.
If multiple Apache Cassandra instances are necessary then test cases should be added to `standalone-integration` or `osgi-integration` tests.

### Checks

All checks needs to pass before merging a pull request.
Sometimes the PMD rules does not match the goal of the project and in those cases it might be relevant to discuss removing or ignoring specific rules/violations.
Currently there is a base of PMD rules used and that will evolve with the project,
so if a rule seems to be detrimental rather helping the project,
feel free to discuss it within the pull request.

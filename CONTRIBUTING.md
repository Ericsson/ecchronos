# Contributing

You are most welcome to create pull requests and issues to ecChronos.
Before creating pull requests it is recommended to first discuss your idea with at least one of the owners of the repository.
For keeping track of the history it is recommended that most communication is performed or summarized within pull requests or issues.

## Development

### Prerequisites

* Maven
* JDK8
* Docker (for test setup)
* Python

### Branches

Target the lowest applicable version of ecChronos when fixing an issue.
Bug fixes should be targeted to the lowest maintained version where the bug reside.
New features should, in general, be added to the master branch.

### Code Style

This project uses the cassandra code style which is based on Sun’s Java coding conventions.
Formatting rules for eclipse can be found [here](code_style.xml).

Provided patches should be contained and should not modify code outside of the scope of the patch.
This will make it quicker to perform reviews and merging the pull requests.

### Builds

If you're looking to contribute to ecChronos it is recommended that you sign up on [Travis](https://travis-ci.org/) and [Coveralls](https://coveralls.io/) and enable builds and reports on your own fork of ecChronos.
The builds required to merge a pull requests are contained within the [Travis configuration](.travis.yml) and include tests, code coverage as well as PMD checks.

All checks needs to pass before merging a pull request.
The current PMD rules are constantly evolving.
If you encounter a PMD rule that seems odd or non-relevant feel free to discuss it within an issue or pull request.

#### Built with

* [Maven](https://maven.apache.org) - Dependency and build management
* [docker-maven-plugin](https://github.com/fabric8io/docker-maven-plugin) - For integration tests

### Tests

Make sure to add tests to the relevant phase(s) when possible.
If mocks or a single Apache Cassandra instance is necessary it may be run as a unit test.
If multiple Apache Cassandra instances are necessary then test cases should be added to `standalone-integration` and/or `osgi-integration` tests.
If HTTP related functionality is changed then `ecchronos-binary` test cases should be changed.

#### Running the tests

There are a few different tests that can be run:

* Unit tests
* Docker tests
  * Integration tests
  * Acceptance tests

The full test suite can be run by `mvn verify -P docker-integration-test,osgi-integration-tests,standalone-integration-tests,python-integration-tests`.
Make sure to add test cases to the appropriate test suite.

##### Unit tests

The unit tests are run by `mvn clean test`.
They are running simple tests that sometimes utilize a single embedded Cassandra node.

##### Docker tests

The acceptance tests and integration tests use docker instances by default.
The docker containers gets started when the `-P docker-integration-test` flag is used in maven.
The docker command must be runnable without *sudo* for the user running the tests.

###### Integration tests

The integration tests tries to start ecChronos with a cluster of nodes and verfiy that repairs are run.
They are activated by using `-P osgi-integration-tests` or `-P standalone-integration-tests`.
It is possible to run either OSGi integration tests or the standalone tests without the other.
This can be done by running either `mvn clean install -P docker-integration-test,osgi-integration-tests` or `mvn clean install -P docker-integration-test,standalone-integration-tests`.

###### Acceptance tests

The acceptance test use behave to verify the python scripts as well as the REST server in ecChronos.
They are activated by using `-P python-integration-tests` in combination with the docker flag.

#### Maven configuration properties

| Property                   | Default    | Description                                              |
|----------------------------|------------|----------------------------------------------------------|
| it.cassandra.memory        | 1073741824 | Memory limit for the docker instance                     |
| it.cassandra.heap          | 256M       | Amount of heap memory Cassandra should use at most       |

#### Running within IDEA/Eclipse

If you already have a three node cluster setup (through docker, ccm, etc) this can be used instead.

Make sure to run the following command (with correct data centers specified in the create_keyspaces.cql) to prepare the cluster.
```
cqlsh -f standalone-integration/src/test/resources/create_keyspaces.cql
```

The test cases needs to be run with the following system properties set:

| Variable                 |  Description                                  |
|--------------------------|-----------------------------------------------|
| it-cassandra.ip          | The host ip of one of the Cassandra instances |
| it-cassandra.native.port | The native port used                          |
| it-cassandra.jmx.port    | The JMX port used                             |

#### Break down into end to end tests

The `standalone-integration` tests runs a setup similar to the standalone application to verify automated repairs.
The `osgi-integration` tests runs in a OSGi environment to verify that all services are available and verifies automated repairs.

## Creating a pull request

1. Ensure that the pull request is targeted at the minimum possible version based on the type of change.
2. If you intend to fix an issue, mention the issue number in the text body of the pull requests, i.e. `Fixes #20`.
3. Add a changelog entry at the bottom of the current target version when handling an issue.
4. All checks must pass before merging a pull request.
5. In general at least one project admin should approve the pull request before merging.

## Merging

Merging is performed from minimum version towards master.
It is the responsibility of the person merging the pull request to make sure it gets merged to master.

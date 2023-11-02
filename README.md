# ecChronos

[![codecov](https://codecov.io/gh/ericsson/ecchronos/branch/ecchronos-2.0/graph/badge.svg)](https://codecov.io/gh/ericsson/ecchronos/tree/ecchronos-2.0)
[![maven central](https://img.shields.io/maven-central/v/com.ericsson.bss.cassandra.ecchronos/ecchronos-binary.svg?label=maven%20central&versionPrefix=2.0)](https://central.sonatype.com/artifact/com.ericsson.bss.cassandra.ecchronos/ecchronos-binary/versions)

ecChronos is a decentralized scheduling framework primarily focused on performing automatic repairs in Apache Cassandra.

The aim of ecChronos is to provide a simple yet effective scheduler that helps in maintaining a cassandra cluster. It is primarily used to run repairs but can be extended to run all manner of maintenance work as well.

* Automate the process of keeping cassandra repaired.
* Split a table repair job into many smaller subrange repairs
* Expose statistics on how well repair is keeping up with the churn of data
* Flexible through many different plug-in points to customize to your specific use case

ecChronos is a helper application that runs next to each instance of Apache Cassandra. It handles maintenance operations for the local node.
The repair tasks make sure that each node runs repair once every interval.
The interval is configurable but defaults to seven days.

More details on the underlying infrastructure can be found in [ARCHITECTURE.md](docs/ARCHITECTURE.md).

More information on the REST interface of ecChronos is described in [REST.md](docs/REST.md).

### Prerequisites

* JDK8

### Installation

Installation instructions can be found in [SETUP.md](docs/SETUP.md).

### Upgrade

Upgrade instructions can be found in [UPGRADE.md](docs/UPGRADE.md).

### Compatibility with Cassandra versions

For information about which ecChronos versions have been tested with which Cassandra versions can be found in [COMPATIBILITY.md](docs/COMPATIBILITY.md)

## Contributing

Please read [CONTRIBUTING.md](docs/CONTRIBUTING.md) for details on our code of conduct, development, and the process for submitting pull requests to us.

## Versioning

We try to adhere to [SemVer](http://semver.org) for versioning.
* Anything requiring changes to configuration or plugin APIs should be released in a new major version.
* Anything extending configuration or plugins in a backwards compatible way should be released in a new minor version.
* Bug fixes should be made for the first known version and merged forward.

## Authors

* **Marcus Olsson** - *Initial work* - [emolsson](https://github.com/emolsson)

See also the list of [contributors](https://github.com/ericsson/ecchronos/contributors) who participated in this project.

## License

This project is licensed under the Apache License - see the [LICENSE.md](LICENSE.md) file for details

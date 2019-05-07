# ecChronos
[![build](https://travis-ci.org/Ericsson/ecchronos.svg?branch=master)](https://travis-ci.org/Ericsson/ecchronos)
[![coverage](https://coveralls.io/repos/github/Ericsson/ecchronos/badge.svg?branch=master)](https://coveralls.io/github/Ericsson/ecchronos?branch=master)
[![maven central](https://img.shields.io/maven-central/v/com.ericsson.bss.cassandra.ecchronos/ecchronos-binary.svg?label=maven%20central)](https://search.maven.org/search?q=g:%22com.ericsson.bss.cassandra.ecchronos%22%20AND%20a:%22ecchronos-binary%22)

ecChronos is a decentralized scheduling framework primarily focused on performing automatic repairs in Apache Cassandra.

## Getting started

See the [STRUCTURE.md](docs/STRUCTURE.md) and [ARCHITECTURE.md](docs/ARCHITECTURE.md) for details on ecChronos project structure and architecture.

### Prerequisites

* JDK8

### Installation

Installation instructions can be found in [SETUP.md](docs/SETUP.md).

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, development, and the process for submitting pull requests to us.

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

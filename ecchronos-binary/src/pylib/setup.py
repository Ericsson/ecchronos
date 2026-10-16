#!/usr/bin/env python3
# vi: syntax=python
#
# Copyright 2025 Telefonaktiebolaget LM Ericsson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from distutils.core import setup

try:
    from ecchronoslib.version import get_client_version

    _RAW_VERSION = get_client_version()
except ImportError:
    _RAW_VERSION = "unknown"


def _to_pep440(raw):
    """Map a Maven version to a PEP 440 version setuptools will accept.

    Maven uses a '-SNAPSHOT' suffix (e.g. '1.1.2-SNAPSHOT') which is not a
    valid PEP 440 version. Map it to a development release ('1.1.2.dev0').
    The sentinel 'unknown' is replaced with '0.0.0' so packaging never fails.
    """
    if not raw or raw == "unknown":
        return "0.0.0"
    if raw.endswith("-SNAPSHOT"):
        return raw[: -len("-SNAPSHOT")] + ".dev0"
    return raw


setup(
    name="ecChronos library",
    version=_to_pep440(_RAW_VERSION),
    description="ecChronos REST library",
    author="Victor Cavichioli",
    author_email="victor.cavichioli@ericsson.com",
    url="https://github.com/Ericsson/ecchronos",
    license="Apache License, Version 2.0",
    packages=["ecchronoslib"],
)

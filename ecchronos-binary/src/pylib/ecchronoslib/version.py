#
# Copyright 2026 Telefonaktiebolaget LM Ericsson
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

"""Resolve the ecctool client version.

The real version is baked in at build time as ``_version.py`` (generated
from ``_version.py.in`` by filtering the Maven ``${project.version}``).
When running from source that file is absent, so fall back to a sentinel.
"""

UNKNOWN_VERSION = "unknown"


def get_client_version():
    """Return the ecctool client version, or "unknown" when running from source."""
    try:
        from ecchronoslib._version import __version__  # pylint: disable=import-outside-toplevel

        if __version__ and not __version__.startswith("${"):
            return __version__
    except ImportError:
        pass
    return UNKNOWN_VERSION

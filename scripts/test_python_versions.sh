#!/bin/bash
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

# -----------------------------------------------------------------------------
# USAGE
#
#   Run all default versions:
#     ./test_python_versions.sh
#
#   Run one or more specific versions (overrides the default list):
#     ./test_python_versions.sh 3.15.0
#     ./test_python_versions.sh 3.14.6 3.15.0
#
#   Same thing via env var instead of arguments:
#     PY_VERSIONS="3.15.0" ./test_python_versions.sh
#
# -----------------------------------------------------------------------------
# GETTING NEW PYTHON VERSIONS (pyenv)
#
#   pyenv only knows about versions shipped with its local python-build plugin,
#   so a freshly released Python won't appear until you update pyenv itself:
#     cd ~/.pyenv && git pull      # or: pyenv update  (if the plugin is installed)
#
#   List versions pyenv can install (filter to e.g. 3.15):
#     pyenv install --list | grep 3.15
#
#   Install a version, then add it to the VERSIONS list below:
#     pyenv install 3.15.0
#
#   Note: a brand-new Python may lack prebuilt wheels for cassandra-driver,
#   forcing a source build that needs libev headers (sudo apt-get install libev-dev).
#   Prefer a version that already has prebuilt wheels when possible.
# -----------------------------------------------------------------------------
#

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

# Versions to test (see USAGE above for how to override).
if [ "$#" -gt 0 ]; then
  VERSIONS=("$@")
elif [ -n "$PY_VERSIONS" ]; then
  # shellcheck disable=SC2206
  VERSIONS=($PY_VERSIONS)
else
  VERSIONS=("system" "3.9.20" "3.13.0" "3.14.6" "3.15.0")
fi
declare -A RESULTS

for ver in "${VERSIONS[@]}"; do
  echo "=========================================="
  echo "Testing with Python: $ver"
  echo "=========================================="
  export PYENV_VERSION="$ver"

  # Verify the version is available before running
  if ! python3 --version &>/dev/null; then
    RESULTS[$ver]="SKIP (not installed)"
    echo "  Skipping — $ver is not installed"
    continue
  fi

  py_actual=$(python3 --version 2>&1)
  echo "$py_actual"

  # Clean up Docker containers from previous run
  docker compose -f ecchronos-binary/target/test/cassandra-test-image/src/main/docker/docker-compose.yml down -v 2>/dev/null || true

  # Remove old venv so it gets recreated with the current Python
  rm -rf ecchronos-binary/target/test/venv

  if mvn verify -P python-integration-tests -Dit.cassandra.version=5.0 -Djava.version=17 -DskipUTs -B; then
    RESULTS[$ver]="PASS ($py_actual)"
  else
    RESULTS[$ver]="FAIL ($py_actual)"
  fi
  echo ""
done

unset PYENV_VERSION

echo "=========================================="
echo "SUMMARY"
echo "=========================================="
for ver in "${VERSIONS[@]}"; do
  echo "  $ver: ${RESULTS[$ver]}"
done

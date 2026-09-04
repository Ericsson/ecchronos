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

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

VERSIONS=("system" "3.9.20" "3.13.0" "3.14.6")
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

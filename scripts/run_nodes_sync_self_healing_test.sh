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

# Runs the nodes_sync self-healing integration test (NodesSyncSelfHealingIT) in the data module.
# It spins up a real Cassandra via Testcontainers (requires a running Docker daemon) and exercises
# the real EccNodesSync write path against the exact production schema. The test covers the legacy
# problem (orphaned rows never cleaned up), the heartbeat + TTL self-healing fix, and staleness
# derivation on the read path.
#
# Usage:
#   ./scripts/run_nodes_sync_self_healing_test.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo ">>> Checking Docker daemon is available (required by Testcontainers)..."
if ! docker ps >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not reachable. Start Docker and retry." >&2
  exit 1
fi

echo ">>> Running nodes_sync self-healing integration test..."
# -am builds the required upstream modules; -Dtest limits to the single test.
# surefire.failIfNoSpecifiedTests=false so the upstream modules (which don't contain
# this test) don't fail the build when the -Dtest filter matches nothing there.
mvn -q -pl data -am \
    -Dtest='NodesSyncSelfHealingIT' \
    -Dsurefire.failIfNoSpecifiedTests=false \
    test

echo ">>> Test run complete."

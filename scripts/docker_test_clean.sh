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

# Inspect or surgically remove the Docker artifacts created by the ecChronos
# integration/acceptance tests. Intended to be run BEFORE a test run (to get a
# clean baseline) and AFTER each run (to see what a profile left behind).
#
# It only touches ecChronos test artifacts: the compose-built cassandra node
# images ('<project>_cassandra-<seed|node>-dcX-rackX-nodeX'), the extra
# topology node image ('cassandra-node3'), the 'ecchronos' image, their
# containers/volumes/networks, and (optionally) the pulled 'cassandra:X.Y'
# base image. Nothing else on the machine is affected.
#
# Usage:
#   scripts/docker_test_clean.sh inspect        # list test artifacts, remove nothing
#   scripts/docker_test_clean.sh clean          # remove test artifacts, KEEP cassandra base image
#   scripts/docker_test_clean.sh clean --base   # also remove the cassandra:X.Y base image
#   scripts/docker_test_clean.sh                # defaults to 'inspect'

set -u

# --- matching patterns -------------------------------------------------------
# Compose-built node images carry a random testcontainers/compose project
# prefix, e.g. 'abc123_cassandra-seed-dc1-rack1-node1'. The extra topology node
# is a fixed 'cassandra-node3' tag. 'ecchronos' is the agent image.
IMAGE_PATTERN='(_cassandra-(seed|node)-dc[0-9]+-rack[0-9]+-node[0-9]+|^cassandra-node3(:|$)|^ecchronos(:|$))'
# The pulled base image, kept by default (only removed with --base).
BASE_IMAGE_PATTERN='^cassandra:'
# Containers created by the harnesses.
CONTAINER_PATTERN='cassandra|ecchronos'
# Volumes: '<project>_cassandra-*-data', 'cassandra-node3-data', etc.
VOLUME_PATTERN='cassandra|node3-data'
# Compose network(s), typically '<project>_cassandra-net'.
NETWORK_PATTERN='cassandra'

MODE="${1:-inspect}"
REMOVE_BASE=false
if [ "${2:-}" = "--base" ]; then
    REMOVE_BASE=true
fi

# Filters a stream of 'repository:tag' lines down to the test-related ones.
# Operates on the repo:tag string so the '^'-anchored patterns match correctly.
image_filter() {
    if [ "$REMOVE_BASE" = true ]; then
        grep -E "${IMAGE_PATTERN}|${BASE_IMAGE_PATTERN}"
    else
        grep -E "${IMAGE_PATTERN}"
    fi
}

print_section() {
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

inspect() {
    print_section "IMAGES (test-related)"
    docker images --format '{{.Repository}}:{{.Tag}}  {{.ID}}  {{.Size}}' \
        | grep -E "${IMAGE_PATTERN}|${BASE_IMAGE_PATTERN}" \
        || echo "  (none)"

    print_section "CONTAINERS (test-related)"
    docker ps -a --format '{{.Names}}\t{{.Image}}\t{{.Status}}' \
        | grep -E "${CONTAINER_PATTERN}" \
        || echo "  (none)"

    print_section "VOLUMES (test-related)"
    docker volume ls --format '{{.Name}}' \
        | grep -E "${VOLUME_PATTERN}" \
        || echo "  (none)"

    print_section "NETWORKS (test-related)"
    docker network ls --format '{{.Name}}' \
        | grep -E "${NETWORK_PATTERN}" \
        || echo "  (none)"
}

clean() {
    echo "Removing ecChronos test containers..."
    docker ps -a --format '{{.ID}} {{.Names}} {{.Image}}' \
        | grep -E "${CONTAINER_PATTERN}" \
        | awk '{print $1}' | xargs -r docker rm -f

    if [ "$REMOVE_BASE" = true ]; then
        echo "Removing ecChronos test images (INCLUDING cassandra base image)..."
    else
        echo "Removing ecChronos test images (keeping cassandra base image)..."
    fi
    # Filter on the 'repository:tag' string (so the '^'-anchored patterns work),
    # then remove by tag. '<none>:<none>' entries (dangling) are skipped here.
    docker images --format '{{.Repository}}:{{.Tag}}' \
        | grep -v '^<none>:<none>$' \
        | image_filter \
        | sort -u | xargs -r docker rmi -f

    echo "Removing ecChronos test volumes..."
    docker volume ls --format '{{.Name}}' \
        | grep -E "${VOLUME_PATTERN}" | xargs -r docker volume rm

    echo "Removing ecChronos test networks..."
    docker network ls --format '{{.ID}} {{.Name}}' \
        | grep -E "${NETWORK_PATTERN}" \
        | awk '{print $1}' | xargs -r docker network rm

    echo
    echo "Cleanup done. Current state:"
    echo
    inspect
}

case "$MODE" in
    inspect)
        inspect
        ;;
    clean)
        clean
        ;;
    *)
        echo "Usage: $0 [inspect|clean] [--base]"
        echo "  inspect        list test artifacts, remove nothing (default)"
        echo "  clean          remove test artifacts, keep cassandra base image"
        echo "  clean --base   also remove the cassandra:X.Y base image"
        exit 1
        ;;
esac

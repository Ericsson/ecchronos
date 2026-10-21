#!/usr/bin/env bash
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

#
# run-ci.sh - Run the CI suites from .github/workflows/actions.yml locally.
#
# This mirrors the "Test report" workflow: it performs the one-time setup
# steps (Python black check + dependency install) and then runs the selected
# test suites. Each suite maps 1:1 to an entry in the workflow matrix.
#
# Usage:
#   ./run-ci.sh                       # run the fast suites (unit + style)
#   ./run-ci.sh --list                # list all available suites
#   ./run-ci.sh all                   # run every suite (slow; needs Docker)
#   ./run-ci.sh unit style            # run specific suites by key
#   ./run-ci.sh --java 21 unit        # use a specific JDK (if JAVA_HOME_21_X64 or similar is set)
#   ./run-ci.sh --skip-setup unit     # skip the black check + dependency install
#   ./run-ci.sh --no-black unit       # skip only the black check
#
# Suite keys (see --list for the full command each runs):
#   unit, style,
#   it-standalone-41, it-standalone-50, it-python, it-topology, it-multi-dc,
#   it-jolokia-standalone, it-jolokia-python, it-jolokia-python-pem,
#   it-jolokia-topology, it-jolokia-topology-pem, it-jolokia-multi-dc,
#   it-jolokia-multi-dc-pem

set -euo pipefail

# Resolve the repo root (this script lives in <repo>/scripts) and run from there,
# so Maven finds the top-level pom.xml regardless of where the script is invoked from.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ---- Maven options copied from the workflow env ----
export MAVEN_OPTS="-Dmaven.wagon.http.retryHandler.class=standard -Dmaven.wagon.http.retryHandler.count=3 -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120"

JAVA_VERSION="17"
RUN_BLACK=1
RUN_SETUP=1
SELECTED=()

# ---- Suite definitions: key -> "Human name|maven goals" ----
# These goals are taken verbatim from the workflow matrix.
declare -A SUITE_NAME
declare -A SUITE_GOALS

add_suite() { SUITE_NAME["$1"]="$2"; SUITE_GOALS["$1"]="$3"; }

add_suite unit                     "Unit tests"                                   'test jacoco:report'
add_suite style                    "Style check"                                  'test-compile com.mycila:license-maven-plugin:check pmd:pmd pmd:cpd pmd:check pmd:cpd-check javadoc:jar'
add_suite it-standalone-41         "Standalone integration 4.1"                   'verify -P standalone-integration-tests -Dit.cassandra.version=4.1 -DskipUTs'
add_suite it-standalone-50         "Standalone integration 5.0"                   'verify -P standalone-integration-tests -Dit.cassandra.version=5.0 -DskipUTs'
add_suite it-python                "Python integration"                           'verify -P python-integration-tests -Dit.cassandra.version=5.0 -DskipUTs'
add_suite it-topology              "Topology integration"                         'verify -P topology-integration-tests -Dit.cassandra.version=5.0 -DskipUTs'
add_suite it-multi-dc              "DatacenterAware Multi-Agent integration"      'verify -P multi-agent-datacenter-aware-integration-tests -Dit.cassandra.version=5.0 -DskipUTs'
add_suite it-jolokia-standalone    "Jolokia Standalone integration 5.0"           'verify -P standalone-integration-tests -Dit.cassandra.version=5.0 -Dit.jolokia.enabled=true -DskipUTs'
add_suite it-jolokia-python        "Jolokia Python integration"                   'verify -P python-integration-tests -Dit.cassandra.version=5.0 -Dit.jolokia.enabled=true -DskipUTs'
add_suite it-jolokia-python-pem    "Jolokia Python integration - PEM"             'verify -P python-integration-tests -Dit.cassandra.version=5.0 -Dit.jolokia.enabled=true -Dit.jmx.pem.enabled=true -DskipUTs'
add_suite it-jolokia-topology      "Jolokia Topology integration"                 'verify -P topology-integration-tests -Dit.cassandra.version=5.0 -Dit.jolokia.enabled=true -DskipUTs'
add_suite it-jolokia-topology-pem  "Jolokia Topology integration - PEM"           'verify -P topology-integration-tests -Dit.cassandra.version=5.0 -Dit.jolokia.enabled=true -Dit.jmx.pem.enabled=true -DskipUTs'
add_suite it-jolokia-multi-dc      "Jolokia DatacenterAware Multi-Agent"          'verify -P multi-agent-datacenter-aware-integration-tests -Dit.cassandra.version=5.0 -Dit.jolokia.enabled=true -DskipUTs'
add_suite it-jolokia-multi-dc-pem  "Jolokia DatacenterAware Multi-Agent - PEM"    'verify -P multi-agent-datacenter-aware-integration-tests -Dit.cassandra.version=5.0 -Dit.jolokia.enabled=true -Dit.jmx.pem.enabled=true -DskipUTs'

# Ordered list of all suite keys (matches workflow order)
ALL_KEYS=(unit style it-standalone-41 it-standalone-50 it-python it-topology it-multi-dc \
          it-jolokia-standalone it-jolokia-python it-jolokia-python-pem \
          it-jolokia-topology it-jolokia-topology-pem it-jolokia-multi-dc it-jolokia-multi-dc-pem)

# Default (when no suite given): the fast, Docker-free suites.
DEFAULT_KEYS=(unit style)

usage() {
    sed -n '2,40p' "$0" | sed 's/^#//; s/^ //'
    exit 0
}

list_suites() {
    echo "Available suites:"
    for k in "${ALL_KEYS[@]}"; do
        printf "  %-24s %s\n" "$k" "${SUITE_NAME[$k]}"
    done
    echo
    echo "Groups: all, default (${DEFAULT_KEYS[*]})"
    exit 0
}

# ---- Parse arguments ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)       usage ;;
        --list)          list_suites ;;
        --java)          JAVA_VERSION="$2"; shift 2 ;;
        --no-black)      RUN_BLACK=0; shift ;;
        --skip-setup)    RUN_SETUP=0; shift ;;
        all)             SELECTED=("${ALL_KEYS[@]}"); shift ;;
        default)         SELECTED=("${DEFAULT_KEYS[@]}"); shift ;;
        -*)              echo "Unknown option: $1" >&2; exit 2 ;;
        *)
            if [[ -z "${SUITE_GOALS[$1]:-}" ]]; then
                echo "Unknown suite: $1 (use --list to see valid keys)" >&2
                exit 2
            fi
            SELECTED+=("$1"); shift ;;
    esac
done

if [[ ${#SELECTED[@]} -eq 0 ]]; then
    SELECTED=("${DEFAULT_KEYS[@]}")
fi

# ---- Resolve JDK for the requested version, if a matching JAVA_HOME is set ----
# GitHub's setup-java exports JAVA_HOME_17_X64 / JAVA_HOME_21_X64 etc. Honour the
# same convention locally if present; otherwise fall back to the current java.
jdk_var="JAVA_HOME_${JAVA_VERSION}_X64"
if [[ -n "${!jdk_var:-}" ]]; then
    export JAVA_HOME="${!jdk_var}"
    export PATH="$JAVA_HOME/bin:$PATH"
fi
echo "==> Using Java: $(java -version 2>&1 | head -1)  (target java.version=$JAVA_VERSION)"

# ---- Track results ----
declare -a RESULTS
overall_rc=0

run_step() {
    local label="$1"; shift
    echo ""
    echo "==================================================================="
    echo "==> $label"
    echo "==> \$ $*"
    echo "==================================================================="
    if "$@"; then
        RESULTS+=("PASS  $label")
    else
        RESULTS+=("FAIL  $label")
        overall_rc=1
    fi
}

# ---- One-time setup (mirrors the workflow's setup steps) ----
if [[ $RUN_SETUP -eq 1 ]]; then
    if [[ $RUN_BLACK -eq 1 ]]; then
        if command -v black >/dev/null 2>&1; then
            # Non-fatal: local black version may differ from CI's (24.10.0).
            echo ""
            echo "==> Black style check (./ecchronos-binary/src, line-length 120)"
            if ! black --check --line-length 120 ./ecchronos-binary/src; then
                echo "WARN: black check failed. Your local black is: $(black --version)."
                echo "WARN: CI uses black 24.10.0 - differences may be version-related. Not failing the run."
            fi
        else
            echo "WARN: black not installed; skipping Python style check."
        fi
    fi

    run_step "install dependencies (build-cassandra-test-jar, skip tests)" \
        mvn install -Pbuild-cassandra-test-jar -DskipTests=true -B
fi

# ---- Run selected suites ----
for key in "${SELECTED[@]}"; do
    name="${SUITE_NAME[$key]}"
    goals="${SUITE_GOALS[$key]}"

    # Topology suites need larger Cassandra heap limits (see workflow).
    if [[ "$name" == *Topology* ]]; then
        export MAX_HEAP_SIZE=1536M
        export HEAP_NEWSIZE=200M
    fi

    # shellcheck disable=SC2086  # goals is intentionally word-split into maven args
    run_step "$name [$key]" \
        mvn $goals -Djava.version="$JAVA_VERSION" -Dnet.bytebuddy.experimental=true -B
done

# ---- Summary ----
echo ""
echo "==================================================================="
echo "Summary"
echo "==================================================================="
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo ""
if [[ $overall_rc -eq 0 ]]; then
    echo "All selected steps passed."
else
    echo "One or more steps failed."
fi
exit $overall_rc

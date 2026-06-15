#!/bin/bash
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

set -e

source variables.sh

echo "Setting up Python environment and dependencies..."

# Setup virtual environment for non-CI environments
if [ -z "${CI}" ] && [ -z "${VIRTUAL_ENV}" ]; then
  echo "Installing virtualenv"
  pip install virtualenv
  virtualenv "$VENV_DIR" --python=python3
  source "$VENV_DIR"/bin/activate
fi

# Safety guard: in non-CI environments, refuse to install into global/user
# site-packages. Installing ecchronoslib globally leaves a stale copy on
# sys.path that shadows src/pylib and breaks pylint (and other tooling).
if [ -z "${CI}" ] && [ -z "${VIRTUAL_ENV}" ]; then
  echo "ERROR: no virtualenv is active and venv setup did not run." >&2
  echo "Refusing to install dependencies into global/user site-packages." >&2
  echo "Activate a virtualenv (or set CI=true for CI runs) and retry." >&2
  exit 1
fi

echo "Installing Python dependencies from requirements.txt"
pip install -r requirements.txt

echo "Installing ecChronos Python library"
BASE_DIR="$TEST_DIR"/ecchronos-binary-agent-${PROJECT_VERSION}
PYLIB_DIR="$BASE_DIR"/pylib

if [ -d "$PYLIB_DIR" ]; then
  pip install "$PYLIB_DIR"
else
  echo "Warning: ecChronos Python library directory not found at $PYLIB_DIR"
fi

echo "All dependencies installed successfully!"
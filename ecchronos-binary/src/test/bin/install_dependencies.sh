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

set -euo pipefail

source variables.sh

echo "Setting up Python environment and dependencies..."

# Use an isolated virtual environment in both CI and local runs to keep
# dependency resolution deterministic and avoid host-environment leakage.
python3 -m venv "$VENV_DIR"
source "$VENV_DIR"/bin/activate

python -m pip install --upgrade pip

echo "Installing Python dependencies from requirements.txt"
python -m pip install -r requirements.txt

echo "Installing ecChronos Python library"
BASE_DIR="$TEST_DIR"/ecchronos-binary-${PROJECT_VERSION}
PYLIB_DIR="$BASE_DIR"/pylib

if [ -d "$PYLIB_DIR" ]; then
  python -m pip install "$PYLIB_DIR"
else
  echo "Warning: ecChronos Python library directory not found at $PYLIB_DIR"
fi

echo "All dependencies installed successfully!"

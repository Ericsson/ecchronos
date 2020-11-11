#!/bin/bash
#
# Copyright 2020 Telefonaktiebolaget LM Ericsson
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

source variables.sh

echo "Installing virtualenv"

# Install virtualenv and pylint
if [ -z "${CI}" ]; then
  pip install --user virtualenv
  virtualenv "$VENV_DIR"
  source "$VENV_DIR"/bin/activate
fi

echo "Installing pylint"

pip install pylint

echo "Installing behave dependencies"

pip install behave
pip install requests
pip install jsonschema

for directory in "$@"
do
  echo "Running pylint for $directory"
  pylint "$directory" || exit 1
done

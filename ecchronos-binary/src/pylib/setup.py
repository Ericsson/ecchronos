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

setup(name='ecChronos library',
      version='1.0',
      description='ecChronos REST library',
      author='Marcus Olsson',
      author_email='marcus.olsson@ericsson.com',
      url='https://github.com/Ericsson/ecchronos',
      license='Apache License, Version 2.0',
      packages=['ecchronoslib'])

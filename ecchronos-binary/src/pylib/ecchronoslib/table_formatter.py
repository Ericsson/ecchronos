#
# Copyright 2019 Telefonaktiebolaget LM Ericsson
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


def calculate_max_len(data, i):
    max_len = 0
    for array in data:
        current_len = len(str(array[i]))
        if current_len > max_len:
            max_len = current_len
    return max_len


def format_table(data):
    if len(data) <= 0:
        return

    print_format = "| "
    total_length = 2

    for idx, _ in enumerate(data[0]):
        max_len = calculate_max_len(data, idx)
        print_format = "{0}{{{1}:{2}s}} | ".format(print_format, idx, max_len)
        total_length += max_len + 3
    total_length -= 1  # Last space is not counted

    print("-" * total_length)
    print(print_format.format(*data[0]))
    print("-" * total_length)

    if len(data) <= 1:
        return

    for array in data[1:]:
        print(print_format.format(*[str(x) for x in array]))
    print("-" * total_length)

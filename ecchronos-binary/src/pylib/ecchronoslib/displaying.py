#
# Copyright 2024 Telefonaktiebolaget LM Ericsson
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

from colorama import Fore, init
from supports_color import supportsColor

init(autoreset=True)

RED = Fore.RED
GREEN = Fore.GREEN
BLUE = Fore.BLUE
YELLOW = Fore.YELLOW
MAGENTA = Fore.MAGENTA
CYAN = Fore.CYAN
RESET = Fore.RESET
DARK_MAGENTA = '\033[0;35m'

color_map = {
    "Id": RED,
    "Host Id": RED,
    "Keyspace": CYAN,
    "Table": CYAN,
    "Status": MAGENTA,
    "Repaired(%)": MAGENTA,
    "Completed at": MAGENTA,
    "Next repair": MAGENTA,
    "Repair type": MAGENTA,
    "Start token": GREEN,
    "End token": RED,
    "Replicas": CYAN,
    "Repaired at": GREEN,
    "Repaired": CYAN,
    "Repair time taken": GREEN,
    "Config": MAGENTA,
    "UUID": GREEN,
    "FLOAT": CYAN,
    "DATETIME": GREEN,
    "INT": YELLOW,
    "TEXT": YELLOW,
    "COMPLETED": GREEN,
    "IN_QUEUE": CYAN,
    "BLOCKED": MAGENTA,
    "WARNING": YELLOW,
    "ERROR": RED,
    "ON_TIME": BLUE,
    "LATE": YELLOW,
    "OVERDUE": YELLOW,
    "Collection": MAGENTA
}

def color_str(field, color, field_type):
    if should_color(color):
        colored_str = color_map[field_type] + str(field) + RESET
        return colored_str
    return field

def color_key(key, color):
    if should_color(color):
        colored_str = color_map[key] + str(key) + RESET
        return colored_str
    return key

def color_index(summary, color):
    if should_color(color):
        colored_summary = []
        for collum in summary:
            colored_collum = color_map[collum] + collum + RESET
            colored_summary.append(colored_collum)
        return colored_summary
    return summary

def verify_system_compatibility() -> bool:
    if supportsColor.stdout:
        return True
    return False

def should_color(color) -> bool:
    should_colorize = False
    if color == "auto":
        should_colorize = verify_system_compatibility()
    if color == "on":
        should_colorize = True
    return should_colorize

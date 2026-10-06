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

"""Client-side filtering of Prometheus/OpenMetrics exposition text."""

from __future__ import print_function


def _normalize_metric_name(value):
    """Normalize a metric name fragment for forgiving comparison."""
    return value.lower().replace(".", "_")


def _sample_metric_name(line):
    """Return the metric name of a sample line, or None for comments/blanks."""
    text = line.strip()
    if not text or text.startswith("#"):
        return None
    end = len(text)
    for sep in ("{", " ", "\t", "="):
        idx = text.find(sep)
        if idx != -1:
            end = min(end, idx)
    return text[:end]


def _comment_metric_name(line):
    """Return the metric name of a '# HELP'/'# TYPE' line, or None."""
    parts = line.strip().split()
    if len(parts) >= 3 and parts[0] == "#" and parts[1] in ("HELP", "TYPE"):
        return parts[2]
    return None


def _make_matcher(normalized):
    """Build a predicate deciding whether a metric name is kept."""

    def matches(metric_name):
        return not normalized or any(fragment in _normalize_metric_name(metric_name) for fragment in normalized)

    return matches


def _keep_comment_line(stripped, normalized, matches):
    """Decide whether a '#' comment/metadata line should be kept.

    The OpenMetrics end marker ('# EOF') is always preserved. HELP/TYPE
    lines are kept when their metric matches the filter; non-metric
    comments are only kept when no filter is active.
    """
    if stripped == "# EOF":
        return True
    metric_name = _comment_metric_name(stripped)
    if metric_name is None:
        return not normalized
    return matches(metric_name)


def filter_metrics_text(scrape_text, names=None, include_comments=True):
    """Filter exposition text client-side by metric name substrings.

    Matching is case-insensitive with '.' and '_' treated as equivalent,
    so a remembered fragment like 'lock.latency' matches
    'ecc_scheduler_lock_latency_seconds'. A line is kept when its metric
    name contains any of the given substrings. Without names the text is
    returned unchanged (unless comments are excluded via --raw).
    """
    normalized = [_normalize_metric_name(name) for name in names] if names else []

    if not normalized and include_comments:
        return scrape_text

    matches = _make_matcher(normalized)

    kept = []
    for line in scrape_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            if include_comments and _keep_comment_line(stripped, normalized, matches):
                kept.append(line)
        elif matches(_sample_metric_name(stripped)):
            kept.append(line)

    if not kept:
        return ""
    result = "\n".join(kept)
    if scrape_text.endswith("\n"):
        result += "\n"
    return result

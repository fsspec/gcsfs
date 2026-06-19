"""Single source of truth for the summary-table columns.

``macrobenchmarks_schema.json`` is consumed directly by ``bq mk`` to define the
external staging table, so it already has to spell out every column and its
BigQuery type. Rather than maintain a second, hand-synced column list in Python
(and a test to police the two), the calculator derives its CSV field order from
that same file. Add a column in one place -- the JSON -- and both the BigQuery
schema and the summary CSV header follow.
"""

import functools
import json
import os

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), os.pardir,
                           "macrobenchmarks_schema.json")


@functools.lru_cache(maxsize=1)
def external_table_definition() -> dict:
    """The parsed ``macrobenchmarks_schema.json`` (``@INFRA_PREFIX@`` intact)."""
    with open(SCHEMA_PATH) as fh:
        return json.load(fh)


@functools.lru_cache(maxsize=1)
def fieldnames() -> list:
    """Summary CSV column names, in BigQuery-schema declaration order."""
    return [field["name"]
            for field in external_table_definition()["schema"]["fields"]]

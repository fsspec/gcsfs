from metrics import summary_schema

NEW_COLUMNS = [
    ("cpu_usage_peak_cores", "FLOAT"),
    ("cpu_usage_mean_cores", "FLOAT"),
    ("memory_usage_peak_bytes", "INTEGER"),
    ("network_received_peak_bytes_per_sec", "FLOAT"),
    ("network_received_mean_bytes_per_sec", "FLOAT"),
    ("network_sent_peak_bytes_per_sec", "FLOAT"),
    ("network_sent_mean_bytes_per_sec", "FLOAT"),
]


def test_new_columns_present_and_typed():
    fields = {
        f["name"]: f["type"]
        for f in summary_schema.external_table_definition()["schema"]["fields"]
    }
    for name, bq_type in NEW_COLUMNS:
        assert name in fields, f"{name} missing from schema JSON"
        assert fields[name] == bq_type, f"{name} should be {bq_type}"


def test_new_columns_are_last_and_in_order():
    names = summary_schema.fieldnames()
    tail = names[-len(NEW_COLUMNS) :]
    assert tail == [name for name, _ in NEW_COLUMNS]

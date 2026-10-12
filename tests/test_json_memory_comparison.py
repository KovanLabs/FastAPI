import json

from scripts.compare_json_memory import custom_records, ijson_records


def test_custom_and_ijson_iterate_the_same_top_level_array(tmp_path) -> None:
    path = tmp_path / "orders.json"
    expected = [{"id": 1}, {"id": 2}, {"id": 3}]
    path.write_text(json.dumps(expected), encoding="utf-8")

    assert list(custom_records(path, maximum_record_bytes=100)) == expected
    assert list(ijson_records(path)) == expected

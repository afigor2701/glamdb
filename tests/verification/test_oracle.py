import pytest


SCHEMA = {"fields": [
    {"name": "n", "type": "int64", "nullable": True},
    {"name": "label", "type": "string", "nullable": False},
]}
ROWS = [{"n": None, "label": "null"}, {"n": 1, "label": "one"}, {"n": 2, "label": "two"}, {"n": 2, "label": "two"}]


@pytest.mark.parametrize("op,value,expected", [
    ("eq", "2", ["two", "two"]), ("ne", "2", ["one"]),
    ("lt", "2", ["one"]), ("le", "2", ["one", "two", "two"]),
    ("gt", "1", ["two", "two"]), ("ge", "2", ["two", "two"]),
    ("eq", None, []),
])
def test_comparison_and_filter_only_column(verify, op, value, expected):
    assert verify.scan_rows(SCHEMA, ROWS, ["label"], {"op": op, "field": "n", "value": value}) == [{"label": x} for x in expected]


@pytest.mark.parametrize("predicate,expected", [
    ({"op": "true"}, ["null", "one", "two", "two"]),
    ({"op": "not", "arg": {"op": "true"}}, []),
    ({"op": "and", "args": [{"op": "true"}, {"op": "eq", "field": "n", "value": "1"}]}, ["one"]),
    ({"op": "or", "args": [{"op": "not", "arg": {"op": "true"}}, {"op": "is_null", "field": "n"}]}, ["null"]),
    ({"op": "in", "field": "n", "values": []}, []),
    ({"op": "in", "field": "n", "values": ["1", None]}, ["one"]),
    ({"op": "not", "arg": {"op": "in", "field": "n", "values": ["1", None]}}, []),
    ({"op": "not", "arg": {"op": "in", "field": "n", "values": []}}, ["one", "two", "two"]),
    ({"op": "is_null", "field": "n"}, ["null"]),
    ({"op": "is_not_null", "field": "n"}, ["one", "two", "two"]),
    ({"op": "or", "args": [{"op": "is_null", "field": "n"}, {"op": "eq", "field": "n", "value": "1"}]}, ["null", "one"]),
    ({"op": "and", "args": [{"op": "is_null", "field": "n"}, {"op": "eq", "field": "n", "value": "1"}]}, []),
])
def test_sql_three_valued_logic(verify, predicate, expected):
    assert verify.scan_rows(SCHEMA, ROWS, ["label"], predicate) == [{"label": x} for x in expected]


def test_projection_limit_initial_empty_and_owned_result(verify):
    assert verify.scan_rows(SCHEMA, ROWS, ["n"], limit=0) == []
    assert verify.scan_rows(SCHEMA, [], ["n"]) == []
    assert verify.scan_rows(SCHEMA, ROWS, [], limit=2) == [{}, {}]
    assert verify.scan_rows(SCHEMA, ROWS, ["label"], {"op": "ge", "field": "n", "value": "2"}, 1) == [{"label": "two"}]
    old = verify.scan_rows(SCHEMA, ROWS, ["label"])
    ROWS.append({"n": 3, "label": "later"})
    try:
        assert old == [{"label": "null"}, {"label": "one"}, {"label": "two"}, {"label": "two"}]
    finally:
        ROWS.pop()


@pytest.mark.parametrize("predicate", [
    {"op": "true", "field": "n"},
    {"op": "eq", "field": "missing", "value": 1},
    {"op": "eq", "field": "n", "value": True},
    {"op": "eq", "field": "n", "value": "1.0"},
    {"op": "in", "field": "n", "values": "1"},
    {"op": "not", "args": []}, {"op": "xor", "args": []},
    {"op": "and", "args": [None]},
    {"op": "and", "args": [{"op": "is_null", "field": "n"}, {"op": "eq", "field": "n", "value": True}]},
])
def test_invalid_predicates_validated_even_empty_or_limit_zero(verify, predicate):
    with pytest.raises(ValueError):
        verify.scan_rows(SCHEMA, [], ["n"], predicate, 0)


@pytest.mark.parametrize("projection,limit", [(["missing"], 0), (["n", "n"], None), ([1], None), (["n"], -1), (["n"], True)])
def test_invalid_scan_shape(verify, projection, limit):
    with pytest.raises(ValueError):
        verify.scan_rows(SCHEMA, [], projection, limit=limit)


@pytest.mark.parametrize("op,true_ids,false_ids", [
    ("and", [0], [2, 5, 6, 7, 8]),
    ("or", [0, 1, 2, 3, 6], [8]),
])
def test_exhaustive_sql_3vl_truth_tables(verify, op, true_ids, false_ids):
    schema = {"fields": [
        {"name": "id", "type": "int64", "nullable": False},
        {"name": "a", "type": "bool", "nullable": True},
        {"name": "b", "type": "bool", "nullable": True},
    ]}
    rows = [{"id": i, "a": a, "b": b} for i, (a, b) in enumerate((a, b) for a in [True, None, False] for b in [True, None, False])]
    predicate = {"op": op, "args": [{"op": "eq", "field": name, "value": True} for name in ["a", "b"]]}
    assert verify.scan_rows(schema, rows, ["id"], predicate) == [{"id": i} for i in true_ids]
    assert verify.scan_rows(schema, rows, ["id"], {"op": "not", "arg": predicate}) == [{"id": i} for i in false_ids]
    dual_op = "or" if op == "and" else "and"
    de_morgan = {
        "op": dual_op,
        "args": [{"op": "not", "arg": {"op": "eq", "field": name, "value": True}} for name in ["a", "b"]],
    }
    assert verify.scan_rows(schema, rows, ["id"], {"op": "not", "arg": predicate}) == verify.scan_rows(schema, rows, ["id"], de_morgan)


def test_exact_timestamp_float_and_binary_string_comparisons(verify):
    schema = {"fields": [
        {"name": "s", "type": "string", "nullable": False},
        {"name": "t", "type": "timestamp_us_utc", "nullable": False},
        {"name": "f", "type": "float64", "nullable": False},
    ]}
    rows = [{"s": "e\u0301", "t": -1, "f": -0.0}, {"s": "é", "t": 0, "f": 1.5}]
    assert verify.scan_rows(schema, rows, ["s"], {"op": "lt", "field": "t", "value": "1970-01-01T00:00:00Z"}) == [{"s": "e\u0301"}]
    assert verify.scan_rows(schema, rows, ["s"], {"op": "eq", "field": "f", "value": 0.0}) == [{"s": "e\u0301"}]
    assert verify.scan_rows(schema, rows, ["s"], {"op": "lt", "field": "s", "value": "é"}) == [{"s": "e\u0301"}]


def test_historical_views_can_exceed_one_append(verify):
    rows = [{"n": 1, "label": "x"}] * 10001
    assert len(verify.scan_rows(SCHEMA, rows, ["n"])) == 10001

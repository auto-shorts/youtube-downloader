import pytest

from auto_shorts.utils import safe_get


@pytest.mark.parametrize(
    "dct,keys,expected",
    [
        ({"a": {"b": {"c": "value"}}}, ("a", "b", "c"), "value"),
        ({"a": {"b": {"c": "value"}}}, ("a",), {"b": {"c": "value"}}),
        ({"a": {"b": {"c": "value"}}}, (), {"a": {"b": {"c": "value"}}}),
        ({}, ("a", "b", "c"), None),
        ({"a": {"b": {"c": "value"}}}, ("a", "b", "x"), None),
        ({"a": {"b": {"c": "value"}}}, ("a", "x", "c"), None),
    ],
)
def test_safe_get_parametrize(dct, keys, expected):
    assert safe_get(dct, *keys) == expected

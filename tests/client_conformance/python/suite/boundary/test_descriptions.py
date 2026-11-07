"""The fields a host expects an endpoint to have, described one type level at a time, the levels a
schema reads back, and the marker of a value the server withheld."""

from __future__ import annotations

import pytest

import nervix_client as nx


def test_a_level_reads_its_type_and_length_and_compares_as_a_value() -> None:
    level = nx.Level(nx.FieldType.FIXED_LIST, 2)
    assert level.type == nx.FieldType.FIXED_LIST
    assert level.length == 2
    assert level == nx.Level(nx.FieldType.FIXED_LIST, 2)
    assert level != nx.Level(nx.FieldType.LIST)
    assert hash(level) == hash(nx.Level(nx.FieldType.FIXED_LIST, 2))
    assert repr(level) == "Level(FieldType.FIXED_LIST, 2)"
    assert nx.Level(nx.FieldType.I16).length == 0


def test_fields_describe_a_nested_type_one_level_at_a_time() -> None:
    # ANCHOR: describe_fields
    fields = nx.Fields()
    fields.add("id", nx.FieldType.U32)
    fields.add("note", nx.FieldType.STRING, nullable=True)
    # ARRAY<I16, 2> is a fixed-size list of two, then its element.
    fields.add("grid", nx.FieldType.FIXED_LIST, 2)
    fields.element(nx.FieldType.I16)
    # VEC<STRING> is a list, then its element.
    fields.add("tags", nx.FieldType.LIST)
    fields.element(nx.FieldType.STRING)
    fields.add("secret", nx.FieldType.STRING, sensitive=True)
    # ANCHOR_END: describe_fields


def test_a_level_that_cannot_be_placed_is_refused_as_an_invalid_argument() -> None:
    fields = nx.Fields()
    with pytest.raises(nx.InvalidArgumentError) as fixed_without_length:
        fields.add("grid", nx.FieldType.FIXED_LIST)
    assert fixed_without_length.value.kind == nx.FailureKind.INVALID_ARGUMENT
    assert "FIXED_LIST" in fixed_without_length.value.message
    with pytest.raises(nx.InvalidArgumentError):
        fields.add("id", nx.FieldType.U32, 4)
    with pytest.raises(nx.InvalidArgumentError):
        fields.add("tags", nx.FieldType.LIST, 1)
    with pytest.raises(nx.InvalidArgumentError):
        fields.add("not a name", nx.FieldType.U32)


def test_every_withheld_value_is_the_one_redacted_marker() -> None:
    assert nx.REDACTED == nx.REDACTED
    assert isinstance(nx.REDACTED, nx.Redacted)
    assert repr(nx.REDACTED) == "REDACTED"
    assert hash(nx.REDACTED) == hash(nx.REDACTED)
    assert nx.REDACTED is not None

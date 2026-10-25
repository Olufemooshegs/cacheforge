import pytest

from cacheforge.keys import KeyBuilder


def test_build_is_deterministic():
    k = KeyBuilder("svc", "v1")
    assert k.build("product", 42) == "svc:v1:product:42"
    assert k.build("product", 42) == k.build("product", 42)


def test_build_normalizes_common_types():
    k = KeyBuilder("svc", "v1")
    assert k.build("flag", True) == "svc:v1:flag:true"
    assert k.build("flag", None) == "svc:v1:flag:null"


def test_namespace_isolation():
    a = KeyBuilder("svc-a", "v1")
    b = KeyBuilder("svc-b", "v1")
    assert a.build("product", 1) != b.build("product", 1)


def test_version_isolation():
    a = KeyBuilder("svc", "v1")
    b = KeyBuilder("svc", "v2")
    assert a.build("product", 1) != b.build("product", 1)


def test_hashed_is_stable_for_equivalent_payloads():
    k = KeyBuilder("svc", "v1")
    assert k.hashed({"a": 1, "b": 2}) == k.hashed({"b": 2, "a": 1})
    assert k.hashed({"a": 1}) != k.hashed({"a": 2})


def test_empty_segment_rejected():
    with pytest.raises(ValueError):
        KeyBuilder("svc").build("")

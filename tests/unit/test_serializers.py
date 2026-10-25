import pytest

from cacheforge.exceptions import CacheSerializationError
from cacheforge.serializers import JsonSerializer


def test_roundtrip_dict():
    s = JsonSerializer()
    assert s.loads(s.dumps({"a": 1, "b": [1, 2]})) == {"a": 1, "b": [1, 2]}


def test_rejects_malformed_payload():
    s = JsonSerializer()
    with pytest.raises(CacheSerializationError):
        s.loads(b"\xff\xfe\x00not-json")


def test_rejects_unserializable_value():
    s = JsonSerializer()
    with pytest.raises(CacheSerializationError):
        s.dumps(object())

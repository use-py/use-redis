import pytest
from use_redis.set import RedisSetStore


@pytest.fixture
def mock_redis(mocker):
    mock = mocker.patch("redis.Redis")
    return mock.return_value


@pytest.fixture
def set_store(mock_redis):
    return RedisSetStore("test_set")


def test_add(set_store, mock_redis):
    set_store.add("value1", "value2")
    mock_redis.sadd.assert_called_once_with("test_set", "value1", "value2")


def test_remove(set_store, mock_redis):
    set_store.remove("value1", "value2")
    mock_redis.srem.assert_called_once_with("test_set", "value1", "value2")


def test_members(set_store, mock_redis):
    mock_redis.smembers.return_value = {"value1", "value2"}
    result = set_store.members()
    assert result == {"value1", "value2"}
    mock_redis.smembers.assert_called_once_with("test_set")


def test_is_member(set_store, mock_redis):
    mock_redis.sismember.return_value = True
    result = set_store.is_member("value1")
    assert result is True
    mock_redis.sismember.assert_called_once_with("test_set", "value1")


def test_size(set_store, mock_redis):
    mock_redis.scard.return_value = 2
    result = set_store.size()
    assert result == 2
    mock_redis.scard.assert_called_once_with("test_set")


def test_pop(set_store, mock_redis):
    mock_redis.spop.return_value = "value1"
    result = set_store.pop()
    assert result == "value1"
    mock_redis.spop.assert_called_once_with("test_set")


def test_move(set_store, mock_redis):
    set_store.move("dst_set", "value1")
    mock_redis.smove.assert_called_once_with("test_set", "dst_set", "value1")


def test_intersection(set_store, mock_redis):
    set_store.intersection("other_set1", "other_set2")
    mock_redis.sinter.assert_called_once_with("test_set", "other_set1", "other_set2")


def test_union(set_store, mock_redis):
    set_store.union("other_set1", "other_set2")
    mock_redis.sunion.assert_called_once_with("test_set", "other_set1", "other_set2")


def test_difference(set_store, mock_redis):
    set_store.difference("other_set1", "other_set2")
    mock_redis.sdiff.assert_called_once_with("test_set", "other_set1", "other_set2")


def test_random_member(set_store, mock_redis):
    mock_redis.srandmember.return_value = "value1"
    result = set_store.random_member()
    assert result == "value1"
    mock_redis.srandmember.assert_called_once_with("test_set")


def test_scan(set_store, mock_redis):
    mock_redis.sscan.return_value = (0, ["value1", "value2"])
    result = set_store.scan(cursor=0, match="val*", count=2)
    assert result == (0, ["value1", "value2"])
    mock_redis.sscan.assert_called_once_with("test_set", 0, "val*", 2)


def test_dynamic_method(set_store, mock_redis):
    set_store.srandmember(3)
    mock_redis.srandmember.assert_called_once_with("test_set", 3)

import pytest
from use_redis.hash import RedisHashStore


@pytest.fixture
def mock_redis(mocker):
    mock = mocker.patch("redis.Redis")
    return mock.return_value


@pytest.fixture
def hash_store(mock_redis):
    return RedisHashStore("test_hash")


def test_set(hash_store, mock_redis):
    hash_store.set("field1", "value1")
    mock_redis.hset.assert_called_once_with("test_hash", "field1", "value1")


def test_set_multiple(hash_store, mock_redis):
    mapping = {"field1": "value1", "field2": "value2"}
    hash_store.set_multiple(mapping)
    mock_redis.hmset.assert_called_once_with("test_hash", mapping)


def test_get_single(hash_store, mock_redis):
    mock_redis.hget.return_value = "value1"
    result = hash_store.get("field1")
    assert result == "value1"
    mock_redis.hget.assert_called_once_with("test_hash", "field1")


def test_get_multiple(hash_store, mock_redis):
    mock_redis.hmget.return_value = ["value1", "value2"]
    result = hash_store.get("field1", "field2")
    assert result == ["value1", "value2"]
    mock_redis.hmget.assert_called_once_with("test_hash", ("field1", "field2"))


def test_delete(hash_store, mock_redis):
    hash_store.delete("field1", "field2")
    mock_redis.hdel.assert_called_once_with("test_hash", "field1", "field2")


def test_exists(hash_store, mock_redis):
    mock_redis.hexists.return_value = True
    result = hash_store.exists("field1")
    assert result is True
    mock_redis.hexists.assert_called_once_with("test_hash", "field1")


def test_length(hash_store, mock_redis):
    mock_redis.hlen.return_value = 2
    result = hash_store.length()
    assert result == 2
    mock_redis.hlen.assert_called_once_with("test_hash")


def test_keys(hash_store, mock_redis):
    mock_redis.hkeys.return_value = ["field1", "field2"]
    result = hash_store.keys()
    assert result == ["field1", "field2"]
    mock_redis.hkeys.assert_called_once_with("test_hash")


def test_values(hash_store, mock_redis):
    mock_redis.hvals.return_value = ["value1", "value2"]
    result = hash_store.values()
    assert result == ["value1", "value2"]
    mock_redis.hvals.assert_called_once_with("test_hash")


def test_items(hash_store, mock_redis):
    mock_redis.hgetall.return_value = {"field1": "value1", "field2": "value2"}
    result = hash_store.items()
    assert result == {"field1": "value1", "field2": "value2"}
    mock_redis.hgetall.assert_called_once_with("test_hash")


def test_increment_int(hash_store, mock_redis):
    mock_redis.hincrby.return_value = 2
    result = hash_store.increment("field1", 1)
    assert result == 2
    mock_redis.hincrby.assert_called_once_with("test_hash", "field1", 1)


def test_increment_float(hash_store, mock_redis):
    mock_redis.hincrbyfloat.return_value = 2.5
    result = hash_store.increment("field1", 1.5)
    assert result == 2.5
    mock_redis.hincrbyfloat.assert_called_once_with("test_hash", "field1", 1.5)


def test_scan(hash_store, mock_redis):
    mock_redis.hscan.return_value = (0, {"field1": "value1", "field2": "value2"})
    result = hash_store.scan(cursor=0, match="f*", count=2)
    assert result == (0, {"field1": "value1", "field2": "value2"})
    mock_redis.hscan.assert_called_once_with("test_hash", 0, "f*", 2)


def test_dynamic_method(hash_store, mock_redis):
    hash_store.hstrlen("field1")
    mock_redis.hstrlen.assert_called_once_with("test_hash", "field1")

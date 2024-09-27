import pytest
from use_redis.list import RedisListStore


@pytest.fixture
def mock_redis(mocker):
    mock = mocker.patch("redis.Redis")
    return mock.return_value


@pytest.fixture
def list_store(mock_redis):
    return RedisListStore("test_list")


def test_lpush(list_store, mock_redis):
    list_store.lpush("value1", "value2")
    mock_redis.lpush.assert_called_once_with("test_list", "value1", "value2")


def test_rpush(list_store, mock_redis):
    list_store.rpush("value1", "value2")
    mock_redis.rpush.assert_called_once_with("test_list", "value1", "value2")


def test_lpop(list_store, mock_redis):
    mock_redis.lpop.return_value = "value1"
    result = list_store.lpop()
    assert result == "value1"
    mock_redis.lpop.assert_called_once_with("test_list")


def test_rpop(list_store, mock_redis):
    mock_redis.rpop.return_value = "value2"
    result = list_store.rpop()
    assert result == "value2"
    mock_redis.rpop.assert_called_once_with("test_list")


def test_length(list_store, mock_redis):
    mock_redis.llen.return_value = 2
    result = list_store.length()
    assert result == 2
    mock_redis.llen.assert_called_once_with("test_list")


def test_range(list_store, mock_redis):
    mock_redis.lrange.return_value = ["value1", "value2"]
    result = list_store.range(0, -1)
    assert result == ["value1", "value2"]
    mock_redis.lrange.assert_called_once_with("test_list", 0, -1)


def test_set(list_store, mock_redis):
    list_store.set(1, "new_value")
    mock_redis.lset.assert_called_once_with("test_list", 1, "new_value")


def test_index(list_store, mock_redis):
    mock_redis.lindex.return_value = "value1"
    result = list_store.index(0)
    assert result == "value1"
    mock_redis.lindex.assert_called_once_with("test_list", 0)


def test_trim(list_store, mock_redis):
    list_store.trim(0, 1)
    mock_redis.ltrim.assert_called_once_with("test_list", 0, 1)


def test_remove(list_store, mock_redis):
    list_store.remove("value1", 1)
    mock_redis.lrem.assert_called_once_with("test_list", 1, "value1")


def test_insert_before(list_store, mock_redis):
    list_store.insert_before("pivot", "new_value")
    mock_redis.linsert.assert_called_once_with("test_list", 'BEFORE', "pivot", "new_value")


def test_insert_after(list_store, mock_redis):
    list_store.insert_after("pivot", "new_value")
    mock_redis.linsert.assert_called_once_with("test_list", 'AFTER', "pivot", "new_value")


def test_dynamic_method(list_store, mock_redis):
    list_store.lmove("source", "destination", "LEFT", "RIGHT")
    mock_redis.lmove.assert_called_once_with("test_list", "source", "destination", "LEFT", "RIGHT")

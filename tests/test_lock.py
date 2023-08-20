import pytest

from usepy_plugin_redis import useRedis, useRedisLock


class TestLock:
    @pytest.fixture
    def lock(self):
        rds = useRedis(
            host="localhost",
            port=6379,
            password=None,
            db=0
        )
        return useRedisLock(rds.connection)

    def test_lock_and_unlock(self, lock):
        key = 'my_key'
        value = 'my_value'
        timeout = 1000

        # 测试 lock() 和 unlock() 方法
        assert lock.lock(key, value, timeout) is True
        assert lock.unlock(key, value) is True

    def test_context_manager(self, lock):
        key = 'my_key'
        value = 'my_value'
        timeout = 2000

        # 测试使用上下文管理器
        with lock(key, value, timeout):
            # 在临界区执行的代码
            assert lock.lock(key, value, timeout) is False

        # 临界区执行完毕后，自动释放锁
        assert lock.lock(key, value, timeout) is True
        assert lock.unlock(key, value) is True
